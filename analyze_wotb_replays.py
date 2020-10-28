#!/usr/bin/env python3.8

# Script Analyze WoT Blitz replays

import sys, argparse, json, os, concurrent, inspect, aiohttp, asyncio, aiofiles, aioconsole
import motor.motor_asyncio, configparser, ssl, re, logging, time, xmltodict, collections
from datetime import datetime, timedelta, date
import blitzutils as bu
from blitzutils import WG
from blitzutils import WoTinspector

logging.getLogger("asyncio").setLevel(logging.DEBUG)

WG_APP_ID  			= 'df25c506c7944e48fbec67e8b74fd34e'
FILE_CONFIG 		= 'blitzstats.ini'
DB_C_TANK_STATS 	= 'WG_TankStats'
DB_C_PLAYER_STATS 	= 'WG_PlayerStats'
DB_C_REPLAYS		= 'Replays'

N_PLAYERS		= 'Nplayers'
MISSING_STATS 	= 'missing_stats'

wg = None
wi = None
REPLAY_N = 0
REPLAY_I = 0
STAT_TANK_BATTLE_MIN = 100
BATTLE_TIME_BUCKET = 3600*24*14

RE_SRC_IS_DB = re.compile(r'^DB:')

## For different player stat functions (global stats, tank tier stats, etc)
# 1st function = for forming stat_id, 2nd for DB stats query, 3rd for WG API stats query
STAT_FUNC	= {
	'tank_tier': 	[ 'get_stat_id_tank_tier', 'get_db_tank_tier_stats', 'get_wg_tank_tier_stats' ],
	'player': 		[ 'get_stat_id_player', 'get_db_player_stats', 'get_wg_player_stats' ]
}

replay_summary_flds = [
	'battle_result',
	'battle_type',
	'map_name',
	'battle_duration',
	'room_type',
	'protagonist',
	'player_name',
	'title', 
	'mastery_badge'
]

replay_details_flds = [
	'damage_made',
	'damage_assisted_track',
	'damage_assisted',
	'damage_received',
	'damage_blocked',
	'enemies_damaged',
	'enemies_destroyed',
	'hitpoints_left',
	'time_alive',
	'distance_travelled',
	'base_capture_points',
	'base_defend_points',
	'wp_points_earned',
	'wp_points_stolen',
	'hits_received',
	'hits_bounced',
	'hits_splash',
	'hits_pen',
	'shots_made',
	'shots_hit',
	'shots_pen',
	'shots_splash',
	'enemies_spotted'
	]


## Syntax: key == stat field in https://api.wotblitz.eu/wotb/tanks/stats/  (all.KEY)
## Value array [ 'Stat Title', [ 0, data buckets ....], scaling_factor_for_bucket_values, 'print_format' ]
histogram_fields = {
	'wins'				: [ 'Win rate', 	[0, .35, .40, .45, .48, .5, .52, .55, .60, .65, .70, 1], 100, '{:2.0f}%' ],
	'damage_dealt'		: [ 'Avg. Dmg.', 	[0, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 100e3], 1, '{:3.0f}' ],
	'battles'			: [ 'Battles', 		[0, 1000, 2500, 5000, 7000, 10e3, 15e3, 25e3, 50e3, 5e7], .001, '{:.0f}k']	# battles is a mandatory stat to include
	}


def def_value_zero():
    return 0

def def_value_BattleRecord():
	return BattleRecord()

class BattleRecordCategory():

	@staticmethod
	def mk_battle_modes(modes: dict):
		"""Make a list of battle modes as required for the _result_categories"""
		mode_ids = modes.values()
		id_max = max(mode_ids)
		ret_modes = ['-'] * (id_max+16)		# create a list of '-' elements
				
		for mode in modes:
			id = modes[mode]
			ret_modes[id] = mode
		
		return ret_modes

	_battle_modes = {
		'Any' 					: 0, 
		'Random'				: 1,
		'Training Room'			: 2,
		'Tournament'			: 4,
		'Rating'				: 7,
		'Mad Games'				: 8,
		'Realistic Battles'		: 22,
		'Gravity Mode'			: 24,
		'Burning Games'			: 26
	}

	_BATTLE_MODES = mk_battle_modes.__func__(_battle_modes)

	_result_categories = {
		'total'				: [ 'TOTAL', 		'total' ],
		'battle_result'		: [ 'Result', 		'category', 	[ 'Loss', 'Win', 'Draw']],
		'battle_type'		: [ 'Battle Type', 	'category', 	['Encounter', 'Supremacy']],
		'tank_tier'			: [ 'Tank Tier', 	'number' ],
		'top_tier'			: [ 'Tier', 		'category', 	['Bottom tier', 'Top tier']],
		'room_type'			: [ 'Battle Mode', 	'category', 	_BATTLE_MODES ],
		'mastery_badge'		: [ 'Battle Medal', 'category', 	['-', '3rd Class', '2nd Class', '1st Class', 'Mastery' ]],
		'team_result'		: [ 'Team Result', 	'string' ],
		'map_name'			: [ 'Map', 			'string' ],
		'tank_name'			: [ 'Tank', 		'string' ],
		'player_name'		: [ 'Player', 		'string' ],
		'protagonist'		: [ 'account_id', 	'number' ],
		'battle_i'			: [ 'Battle #', 	'number' ], 
		'player_wins'		: [ 'Player WR', 	'bucket', [ 0, .35, .45, .50, .55, .65], '%'],
		'player_battles'	: [ 'Player Battles', 	'bucket', [ 0, 500, 1000, 2500, 5e3, 10e3, 15e3, 25e3]],
		'player_damage_dealt'	: [ 'Player Avg Dmg', 	'bucket', [ 0, 500, 1000, 1250, 1500, 1750, 2000, 2500]]
		}

	_result_categories_default = [
		'total',
		'battle_result',
		'battle_type',
		'tank_tier', 
		'top_tier'
		]

	# _result_categories_extra = [
	# 	'mastery_badge',	
	# 	'room_type',
	# 	'tank_name',
	# 	'map_name', 
	# 	'team_result',
	# 	'battle_i',
	# 	'player_wins',
	# 	'player_name',
	# 	'protagonist'		
	# 	]

	total_battles = 0

	RESULT_CAT_FRMT = '{:>20s}'
	
	
	@classmethod
	def get_categories_all(cls):
		return cls._result_categories.keys()


	@classmethod
	def get_default_categories(cls):
		return cls._result_categories_default


	@classmethod
	def get_result_categories(cls, default_cats: list, args : argparse.Namespace):
		extra_cats = args.extra
		only_extra = args.only_extra

		if only_extra:
			res_categories = list()
		else:
			if default_cats == None:
				res_categories = cls.get_default_categories()
			else:
				res_categories = default_cats
		if extra_cats != None: 
			res_categories = res_categories + extra_cats
		res_categories = list(set(res_categories))  	# remove duplicates
				
		return [ cat for cat in cls._result_categories if cat in res_categories ]



	@classmethod
	def get_extra_categories(cls) -> list:
		#return cls._result_categories_extra
		return list( set(cls._result_categories.keys() ) - set(cls.get_default_categories() ) )


	def __init__(self, cat_name : str):
		self.category_name = cat_name
		self.bucket_labels = list()
		self.categories = collections.defaultdict(def_value_BattleRecord)
		if self._result_categories[self.category_name][1] == 'string':
			self.type = 'string'
		elif self._result_categories[self.category_name][1] == 'number':
			self.type = 'number'
		elif self._result_categories[self.category_name][1] == 'total':
			self.type = 'total'
		elif self._result_categories[self.category_name][1] == 'bucket':
			self.type = 'bucket'
			self.bucket_format = self.get_bucket_label_format()
			self.buckets = self.get_bucket_breaks()
			for start in range(len(self.buckets)):
				self.bucket_labels.append(self.mk_bucket_label(start, self.buckets))

		else:
			self.type = 'category'
	

	def get_sub_categories(self):
		return self.categories


	def get_fields(self):
		"""Return fields as dict"""
		key = next(iter(self.get_sub_categories()))
		sub_category = self.categories[key]
		fields = dict()
		for col in sub_category.get_result_fields():
			fields[col] = sub_category.get_field_name(col)
		return fields

	
	def get_category_name(self) -> str:
		return self._result_categories[self.category_name][0]


	def get_category(self, id):
		"""Get category of 'category' type of BattleRecordCategory"""
		try:
			return self._result_categories[self.category_name][2][id]
		except Exception as err:
			bu.error(exception=err)	
		return None


	def get_bucket_breaks(self) -> list:
		"""Return bucket breaks"""
		try:
			return sorted(self._result_categories[self.category_name][2])
		except Exception as err:
			bu.error(exception=err)
		return None

	
	def get_bucket_label_format(self) -> str:
		"""Return bucket label type, if any"""
		try:
			if len(self._result_categories[self.category_name]) > 3:
				return self._result_categories[self.category_name][3]
			return ''			
		except Exception as err:
			bu.error(exception=err)
		return None


	def get_bucket(self, result: dict) -> str:
		try:
			ndx = self.find_bucket(result[self.category_name], self.buckets)
			return self.bucket_labels[ndx]			
		except Exception as err:
			bu.error('Category: ' + self.category_name)
			bu.error('Unknown error', exception=err) 
		return None


	def find_bucket(self, value, buckets):
		"""Find bucket for value"""
		max = len(buckets)
		if max <= 1:
			return 0
		ndx = int(max//2)
		if value > buckets[ndx]:
			return ndx + self.find_bucket(value, buckets[ndx:])
		elif value < buckets[ndx]:
			return self.find_bucket(value, buckets[:ndx])
		else:
			return ndx


	def mk_bucket_label(self, ndx: int, buckets: list) -> str:
		try:
			low = buckets[ndx]
			if ndx >= len(buckets)-1:
				high = None
			else:
				high = buckets[min(ndx+1, len(buckets)-1)]
			
			prefix = ''
			if self.bucket_format == '%':
				frmt = '{:2.0f}'
				if low != None:
					low *= 100
				if high != None:
					high *= 100
				prefix = '%'
			elif self.bucket_format == 'ratio':
				frmt = '{:.2f}'
			else:
				frmt = '{:.0f}'

			label = frmt.format(low) + ' - '
			if high != None:
				label = label + frmt.format(high)
			else:
				label = label + '  '
			label = label + prefix
			return label
		except KeyError as err:
			bu.error('KeyError: ndx=' + str(ndx) + ' buckets=' + str(buckets), exception=err)
		except Exception as err:			
			bu.error('Unknown error', exception=err) 
		return None


	def get_sub_category(self, result: dict) -> str: 
		"""Get category"""
		try:
			cat = None
			if self.type == 'total':
				cat = 'Total'
			elif self.type == 'number':
				cat = str(result[self.category_name])
			elif self.type == 'string':
				cat = result[self.category_name]
			elif self.type == 'category':
				cat = self.get_category(result[self.category_name])
				#cat = self._result_categories[self.category_name][2][result[self.category_name]]
			elif self.type == 'bucket':
				cat = self.get_bucket(result)				
			return cat
		except KeyError as err:
			bu.error('Category: ', str(cat))
			bu.error('Key not found', exception=err)			
		except Exception as err:
			bu.error('Category: ', str(cat))
			bu.error('Unknown error', exception=err) 
		return None
		

	def record_result(self, result: dict):
		try:
			cat = self.get_sub_category(result)
			self.categories[cat].record_result(result)
			self.total_battles += 1
			return True
		except KeyError as err:
			bu.error('Key not found', err)
			bu.error('Category: ', str(cat))
			bu.error(exception = err)
		except Exception as err:
			bu.error('Category: ', str(cat))
			bu.error(exception = err) 
		return False
	

	def calc_results(self):
		try:
			for sub_cat in self.get_sub_categories().keys():
				self.categories[sub_cat].calc_results(self.total_battles)
		except Exception as err:
			bu.error(exception = err)


	def print_results(self):
		try:
			first_btl_record = list(self.categories.values())[0]
			print('   '.join(first_btl_record.get_headers(self.get_category_name())))
			for row in self.get_results():
				print(' : '.join(row))
		except KeyError as err:
			bu.error('Key not found', err)  
		except Exception as err:
			bu.error(exception = err) 
		return None


	def get_results(self):
		try:
			results = []
			# results.append(self.get_headers())			
			if self.type == 'number':
				for cat in sorted( [ int(s) for s in self.categories.keys() ] ):
					cat = str(cat) 
					row = [ self.RESULT_CAT_FRMT.format(cat) ]
					row.extend(self.categories[cat].get_results())
					results.append(row)
			if self.type == 'bucket':
				for cat in [ c for c in self.bucket_labels if c in self.categories.keys()]:
					row = [ self.RESULT_CAT_FRMT.format(cat) ]
					row.extend(self.categories[cat].get_results())
					results.append(row)
			else:
				for cat in sorted(self.categories.keys() , key=str.casefold):
					row = [ self.RESULT_CAT_FRMT.format(cat) ]
					row.extend(self.categories[cat].get_results())
					results.append(row)
			return results
		except KeyError as err:
			bu.error('Key not found', err)  
		return None


	def get_results_json(self):
		"""Get results in JSON format"""
		try:
			results = dict()
			results['name' ] = self.get_category_name()
			# results.append(self.get_headers())			
			if self.type == 'number':
				for cat in sorted( [ int(s) for s in self.categories.keys() ] ):
					cat = str(cat) 
					results[cat] = self.categories[cat].get_results_json()					
			else:
				for cat in sorted(self.categories.keys() , key=str.casefold):
					cat = str(cat) 
					results[cat] = self.categories[cat].get_results_json()					
			return results
		except KeyError as err:
			bu.error('Key not found', err)  
		return None

class BattleRecord():
	
	## Syntax: Check how the replay JSON files look. The algorithm is counting/recording fields
	_result_fields = {
		'battles'			: [ 'Battles', 'Number of battles', 								8, '{:^8.0f}' ],
		'battles%'			: [ '% Battles', 'Share of Battles',								6, '{:6.0%}' ],
		'win'				: [ 'WR', 'Win rate', 												6, '{:6.1%}' ],
		'damage_made'		: [ 'DPB', 'Average Damage', 										5, '{:5.0f}' ],
		'DR'				: [ 'DR', 'Damage Ratio', 											5, '{:5.1f}' ],
		'KDR'				: [ 'KDR', 'Kills / Death', 										4, '{:4.1f}' ],
		'enemies_spotted'	: [ 'Spot', 'Enemies spotted per battle', 							4, '{:4.1f}' ],
		'hit_rate'			: [ 'Hit rate', 'Shots hit / all shots made', 						8, '{:8.1%}' ],
		'pen_rate'			: [ 'Pen rate', 'Shots pen / shots hit', 							8, '{:8.1%}' ],
		'survived'			: [ 'Surv%', 'Survival rate', 										6, '{:6.1%}' ],
		'time_alive%'		: [ 'T alive%', 'Percentage of time being alive in a battle', 		8, '{:8.0%}' ], 
		'top_tier'			: [ 'Top tier', 'Share of games as top tier', 						8, '{:8.0%}' ],
		'player_wins'		: [ 'Player WR', 'Average WR of the player', 						9, '{:9.2%}' ],
		'player_battles'	: [ 'Player Btls', 'Average number battles of the player', 			11, '{:11.0f}' ],
		'allies_wins'		: [ 'Allies WR', 'Average WR of allies at the tier of their tank', 	9, '{:9.2%}' ],
		'enemies_wins'		: [ 'Enemies WR', 'Average WR of enemies at the tier of their tank', 10, '{:10.2%}' ],
		'allies_battles'	: [ 'Allies Btls', 'Average number battles of the allies', 			11, '{:11.0f}' ],
		'enemies_battles'	: [ 'Enemies Btls', 'Average number battles of the enemies', 		12, '{:12.0f}' ],
		'allies_damage_dealt'	: [ 'Allies Avg Dmg', 'Average damage of the allies', 			11, '{:11.0f}' ],
		'enemies_damage_dealt'	: [ 'Enemies Avg Dmg', 'Average damage of the enemies', 		12, '{:12.0f}' ],
		MISSING_STATS		: [ 'No stats', 'Players without stats avail', 						8, '{:8.1%}']		
	}

	_team_fields = [ 'wins', 'battles', 'damage_dealt' ]

	# fields to display in results
	_result_fields_default = [
		'battles',
		'battles%',
		'win',
		'damage_made',
		'player_wins',
		'allies_wins',
		'enemies_wins',
		'allies_battles',
		'enemies_battles'
	]


	_result_fields_team = [
		'battles',
		'player_wins',
		'allies_wins',
		'enemies_wins',
		'allies_damage_dealt',
		'enemies_damage_dealt',
		'allies_battles',
		'enemies_battles'
	]


	_result_fields_extended = [
		'enemies_spotted',
		'top_tier',
		'DR',
		'KDR',
		'hit_rate',
		'pen_rate',
		'survived',
		'time_alive%',
		'player_battles',
		'allies_damage_dealt',
		'enemies_damage_dealt',
		MISSING_STATS
	]

	_result_counts = [
		'battles', 
		MISSING_STATS, 
		'mastery_badge'
	]
	_result_ratios = {
		'KDR'				: [ 'enemies_destroyed', 'destroyed' ],
		'DR'				: [ 'damage_made', 'damage_received' ],
		'hit_rate'			: [ 'shots_hit', 'shots_made' ],
		'pen_rate'			: [ 'shots_pen', 'shots_hit' ],
		'dmg_block_rate' 	: [ 'damage_blocked', 'damage_received' ]
	}

	# _extended_stats = False

	result_fields 	= list()
	avg_fields 		= set()
	ratio_fields 	= set()
		
	RESULT_CAT_HEADER_FRMT = '{:_<20s}'


	@classmethod
	def set_fields(cls, mode: str = None):
		try:
			cls._mode = mode
			
			if cls._mode == OPT_MODE_EXTENDED:
				cls.result_fields  = cls._result_fields_default + cls._result_fields_extended
			elif cls._mode == OPT_MODE_TEAM:
				cls.result_fields = cls._result_fields_team
			else:
				cls.result_fields  = cls._result_fields_default
			cls.result_fields_ratio = set(cls._result_ratios.keys()) & set(cls.result_fields)

			# sort results fields according to _results_fields		
			cls.result_fields.sort(key = lambda i: list(cls._result_fields.keys()).index(i)) 
			
			cls.avg_fields = set(cls.result_fields)
			for field in (set(cls._result_counts) & cls.avg_fields):
				cls.avg_fields.remove(field)
			cls.avg_fields.difference_update(set(cls._result_ratios.keys()))
			for ratio in cls.result_fields_ratio:
				cls.ratio_fields.add(cls._result_ratios[ratio][0])
				cls.ratio_fields.add(cls._result_ratios[ratio][1])
		except Exception as err:
			bu.error('BattleRecord:' ,exception=err)
		

	@classmethod
	def get_result_fields(cls) -> set:
		return cls.result_fields


	@classmethod
	def get_field_name(cls, field: str):
		try:
			return cls._result_fields[field][0]
		except Exception as err:
			bu.error(exception=err)

	@classmethod
	def get_field_width(cls, field: str):
		try:
			return cls._result_fields[field][2]
		except Exception as err:
			bu.error(exception=err)


	@classmethod
	def get_fields_avg(cls) -> set:
		return cls.avg_fields


	@classmethod
	def get_fields_ratio(cls) -> set:
		return cls.ratio_fields


	@classmethod
	def get_team_fields(cls) -> list:
		return cls._team_fields


	def __init__(self):
		try:
			
			self.battles 		= 0
			self.missing_stats 	= 0
			self.n_players 		= 0
			self.ratios_ready 	= False
			self.results_ready 	= False
			self.results = collections.defaultdict(def_value_zero)

		except KeyError as err:
			bu.error('Key not found', err) 


	def record_result(self, result : dict) -> bool:
		try:
			for field in self.avg_fields | self.ratio_fields:
				if (field in result) and (result[field] != None):
					self.results[field] += result[field]
			self.battles 		+= 1
			self.n_players		+= result[N_PLAYERS]
			self.missing_stats 	+= result[MISSING_STATS]
			return True
		except KeyError as err:
			bu.error('Key not found', err)  
			bu.error(str(result))
		except Exception as err:
			bu.error('BattleRecord:' ,exception=err)
		return False


	def calc_ratios(self) -> bool:
		if self.ratios_ready:
			return True
		try:					
			for field in self.result_fields_ratio:
				if self.results[self._result_ratios[field][1]] != 0:
					self.results[field] = self.results[self._result_ratios[field][0]] / self.results[self._result_ratios[field][1]]
				else:
					self.results[field] = float('Inf')
			self.ratios_ready = True
			return True
		except KeyError as err:
			bu.error('Key not found', err) 
		return False


	def calc_results(self, total_battles: int = None):
		if self.results_ready == True: 
			return True
		if not self.ratios_ready:
			self.calc_ratios()
		try:
			for field in self.avg_fields:
				self.results[field] = self.results[field] / max(self.battles,1)
			self.results['battles'] = self.battles
			self.results['battles%'] = self.battles / total_battles
			self.results[MISSING_STATS] = self.missing_stats / max(self.n_players, 1)
			self.results_ready = True
			return True
		except KeyError as err:
			bu.error('Key not found', err) 
		except Exception as err:
			bu.error(exception=err) 
		return False


	def get_headers(self, cat_name: str):
		try:
			headers = [ self.RESULT_CAT_HEADER_FRMT.format(cat_name) ]
			for field in self.result_fields:
				print_format = '{:^' + str(self.get_field_width(field)) + 's}'
				headers.append(print_format.format(self.get_field_name(field)))
			return headers
		except KeyError as err:
			bu.error('Key not found', err)  
		except Exception as err:
			bu.error(str(err)) 
		return None


	def get_results(self) -> list:
		"""Return results for printing"""
		if not self.results_ready:
			bu.error('Stats have not been calculated yet. call calc_results() before get_results()')
		try:
			results = []
			for field in self.result_fields:
				results.append(self._result_fields[field][3].format(self.results[field]) )
			return results
		except KeyError as err:
			bu.error('Key not found', err)  
		except Exception as err:
			bu.error(exception=err) 
		return None
	

	def get_results_json(self) -> dict:
		"""Return results in JSON format"""
		if not self.results_ready:
			bu.error('Stats have not been calculated yet. call calc_results() before get_results()')
		try:
			results = dict()
			for field in self.result_fields:
				results[field] = self.results[field]				
			return results
		except KeyError as err:
			bu.error('Key not found', err)  
		except Exception as err:
			bu.error(exception=err) 
		return None
	


	def print_results(self):
		print(' : '.join(self.get_results()))


class PlayerHistogram():
	def __init__(self, field: str, name: str, fields : list, factor: float, format: str ):
		self.field 		= field
		self.name 		= name
		self.fields 	= fields
		self.cat_factor = factor
		self.cat_format = format
		self.ncat 		= len(self.fields) - 1
		self.allies 	= [0] * self.ncat
		self.enemies 	= [0] * self.ncat
		self.total 		= [0] * self.ncat
	
	def record_ally(self, stat: float):
		for cat in range(0, self.ncat):
			if self.fields[cat] <= stat <= self.fields[cat+1]:
				self.allies[cat] += 1
				self.total[cat]  += 1
				return True
		return False
	
	def record_enemy(self, stat: float):
		for cat in range(0, self.ncat):
			if self.fields[cat] <= stat <= self.fields[cat+1]:
				self.enemies[cat] += 1
				self.total[cat]  += 1
				return True
		return False

	def get_category_name(self, cat: int):
		if cat < self.ncat - 1:
			return self.format_category(cat) + ' - ' + self.format_category(cat+1)
		else:
			return self.format_category(cat) + ' -'


	def format_category(self, cat: int):
		val = self.fields[cat]
		return self.cat_format.format(float(val * self.cat_factor))


	def print(self, ret_json: bool = False):
		N_enemies = 0
		N_allies = 0
		res = dict()
		# calculate buckets 
		for cat in range(0, self.ncat):
			N_enemies += self.enemies[cat]
			N_allies += self.allies[cat]
		N_total = N_allies + N_enemies

		print("\n{:12s} | {:13s} | {:13s} | {:13s}".format(self.name, "Allies", "Enemies", "TOTAL"))
		for cat in range(0, self.ncat):
			print("{:12s} | {:5d} ({:4.1f}%) | {:5d} ({:4.1f}%) | {:5d} ({:4.1f}%)".format(self.get_category_name(cat), self.allies[cat], self.allies[cat]/N_allies*100, self.enemies[cat], self.enemies[cat]/N_enemies*100, self.allies[cat] + self.enemies[cat], (self.allies[cat] + self.enemies[cat])/N_total*100 ))
			if ret_json:
				stat = dict()
				stat['allies'] 		= self.allies[cat]
				stat['allies%'] 	= stat['allies'] / N_allies
				stat['enemies'] 	= self.enemies[cat]
				stat['enemies%'] 	= stat['enemies'] / N_enemies
				stat['total'] 		= self.allies[cat] + self.enemies[cat]
				stat['total%'] 		= stat['total'] / N_total
				res[self.get_category_name(cat)] = stat
			
		return res


class ErrorCatchingArgumentParser(argparse.ArgumentParser):
	def exit(self, status=0, message=None):
		if status:
			if message != None:
				raise UserWarning(message)
			else:
				raise UserWarning()

## main() -------------------------------------------------------------

OPT_MODE_DEFAULT 	= None
OPT_MODE_TEAM 		= 'team'
OPT_MODE_EXTENDED 	= 'extended'
OPT_MODE_HELP		= 'help'
OPT_MODES = [ None, OPT_MODE_TEAM, OPT_MODE_EXTENDED, OPT_MODE_HELP ]

async def main(argv):
	global wg, wi, WG_APP_ID
	# set the directory for the script
	current_dir = os.getcwd()
	os.chdir(os.path.dirname(sys.argv[0]))

	## Default options:
	OPT_DB			= False
	OPT_HIST		= False
	OPT_STAT_FUNC	= 'player'
	OPT_WORKERS_N 	= 10
	OPT_CATEGORIES  = None

	WG_ACCOUNT 		= None 	# format: nick@server, where server is either 'eu', 'ru', 'na', 'asia' or 'china'. 
	  					 	# China is not supported since WG API stats are not available there
	WG_ID			= None  # WG account_id in integer format. 
							# WG_ACCOUNT will be used to retrieve the account_id, but it can be set directly too
	# WG_APP_ID		= WG_APP_ID
	WG_RATE_LIMIT	= 10  ## WG standard. Do not edit unless you have your
						  ## own server app ID, it will REDUCE the performance
	
	## VERY unlikely you have a DB set up
	DB_SERVER 	= 'localhost'
	DB_PORT 	= 27017
	DB_SSL		= False
	DB_CERT_REQ = ssl.CERT_NONE
	DB_AUTH 	= 'admin'
	DB_NAME 	= 'BlitzStats'
	DB_USER		= 'mongouser'
	DB_PASSWD 	= 'PASSWORD'
	DB_CERT 	= None
	DB_CA 		= None
	
	try:
		## Read config
		if os.path.isfile(FILE_CONFIG):
			config = configparser.ConfigParser()
			config.read(FILE_CONFIG)

			try:
				if 'ANALYZER' in config.sections():
					configAnalyzer	= config['ANALYZER']
					OPT_MODE 		= configAnalyzer.getboolean('opt_mode', OPT_MODE_DEFAULT)
					OPT_HIST		= configAnalyzer.getboolean('opt_hist', OPT_HIST)
					OPT_STAT_FUNC	= configAnalyzer.get('opt_stat_func', fallback=OPT_STAT_FUNC)
					OPT_WORKERS_N 	= configAnalyzer.getint('opt_workers', OPT_WORKERS_N)
					res_categories  = configAnalyzer.get('opt_categories', None)
					if res_categories != None:
						OPT_CATEGORIES = res_categories.split(',')
					histogram_fields_str = configAnalyzer.get('histogram_buckets', None)
					if histogram_fields_str != None:
						set_histogram_buckets(json.loads(histogram_fields_str))

			except (KeyError, configparser.NoSectionError) as err:
				bu.error(exception=err)

			try:
				if 'WG' in config.sections():
					configWG 		= config['WG']
					# WG account id of the uploader: 
					# # Find it here: https://developers.wargaming.net/reference/all/wotb/account/list/
					WG_ID			= configWG.getint('wg_id', WG_ID)
					WG_ACCOUNT		= configWG.get('wg_account', WG_ACCOUNT)
					WG_APP_ID		= configWG.get('wg_app_id', WG_APP_ID)
					WG_RATE_LIMIT	= configWG.getint('wg_rate_limit', WG_RATE_LIMIT)
			except (KeyError, configparser.NoSectionError) as err:
				bu.error(exception=err)

			try:
				if 'DATABASE' in config.sections():
					configDB 	= config['DATABASE']
					OPT_DB		= configDB.getboolean('opt_DB', OPT_DB)
					DB_SERVER 	= configDB.get('db_server', DB_SERVER)
					DB_PORT 	= configDB.getint('db_port', DB_PORT)
					DB_SSL		= configDB.getboolean('db_ssl', DB_SSL)
					DB_CERT_REQ = configDB.getint('db_ssl_req', DB_CERT_REQ)
					DB_AUTH 	= configDB.get('db_auth', DB_AUTH)
					DB_NAME 	= configDB.get('db_name', DB_NAME)
					DB_USER		= configDB.get('db_user', DB_USER)
					DB_PASSWD 	= configDB.get('db_password', DB_PASSWD)
					DB_CERT 	= configDB.get('db_ssl_cert_file', DB_CERT)
					DB_CA 		= configDB.get('db_ssl_ca_file', DB_CA)
			except (KeyError, configparser.NoSectionError)  as err:
				bu.error(exception=err)

		parser = ErrorCatchingArgumentParser(description='Analyze Blitz replay JSON files from WoTinspector.com. Use \'upload_wotb_replays.py\' to upload the replay files first.')

		parser.add_argument('--output', default='plain', choices=['plain', 'db'], help='Select output mode: plain text or database')
		parser.add_argument('-id', dest='account_id', type=int, default=WG_ID, help='WG account_id to analyze')
		parser.add_argument('-a', '--account', type=str, default=WG_ACCOUNT, help='WG account nameto analyze. Format: ACCOUNT_NAME@SERVER')
		parser.add_argument('--mode', default=OPT_MODE, choices=OPT_MODES, help='Select stats mode. Options: ' + ', '.join(OPT_MODES[1:]))
		parser.add_argument('--extra', choices=BattleRecordCategory.get_categories_all(), default=None, nargs='*', help='Print extra categories: ' + ', '.join( cat + '=' + BattleRecordCategory._result_categories[cat][0]  for cat in BattleRecordCategory.get_categories_all()))
		parser.add_argument('--only_extra', action='store_true', default=False, help='Print only the extra categories')
		parser.add_argument('--hist', action='store_true', default=OPT_HIST, help='Print player histograms: ' + ', '.join( histogram_fields[k][0] for k in histogram_fields))
		parser.add_argument('--stat_func', default=OPT_STAT_FUNC, choices=STAT_FUNC.keys(), help='Select how to calculate for ally/enemy performance: tank-tier stats, global player stats')
		parser.add_argument('-u', '--url', action='store_true', default=False, help='Print replay URLs')
		parser.add_argument('--tankfile', type=str, default='tanks.json', help='JSON file to read Tankopedia from. Default is "tanks.json"')
		parser.add_argument('--mapfile', type=str, default='maps.json', help='JSON file to read Blitz map names from. Default is "maps.json"')
		parser.add_argument('-j', '--json', action='store_true', default=False, help='Export data in JSON')
		parser.add_argument('-o','--outfile', type=str, default='-', metavar="OUTPUT", help='File to write results. Default STDOUT')
		parser.add_argument('--db', action='store_true', default=OPT_DB, help='Use DB - You are unlikely to have it')
		parser.add_argument('--filters', type=str, default=None, help='Filters for DB based analyses. MongoDB find() filter JSON format. see --help_filters')
		parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
		parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
		parser.add_argument('-s', '--silent', action='store_true', default=False, help='Silent mode')
		parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='+', help='Files/dirs to read. Use \'-\' for STDIN, "db:" for database')
		## parser.add_argument('--help_filters', action='store_true', default=False, help='Extended help for --filters')		

		try:
			args = parser.parse_args()
		except Exception as err:
			raise

		res_categories = BattleRecordCategory.get_result_categories(OPT_CATEGORIES, args)

		bu.set_log_level(args.silent, args.verbose, args.debug)
		bu.set_progress_step(250)  						# Set the frequency of the progress dots. 
		
		wg = WG(WG_APP_ID, args.tankfile, args.mapfile, stats_cache=True, rate_limit=WG_RATE_LIMIT)
		wi = WoTinspector(rate_limit=10)

		if args.account != None:
			args.account_id = await wg.get_account_id(args.account)
			bu.debug('WG  account_id: ' + str(args.account_id))

		BattleRecord.set_fields(args.mode)

		#### Connect to MongoDB (TBD)
		bu.debug('DB_SERVER: ' + DB_SERVER)
		bu.debug('DB_PORT: ' + str(DB_PORT))
		bu.debug('DB_SSL: ' + "True" if DB_SSL else "False")
		bu.debug('DB_AUTH: ' + DB_AUTH)
		bu.debug('DB_NAME: ' + DB_NAME)
		
		client = None
		db = None
		if args.db:
			try:
				client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)
				db = client[DB_NAME]
				args.account_id = None
				bu.debug('Database connection initiated')
			except Exception as err: 
				bu.error("Could no initiate DB connection: Disabling DB", err) 
				args.db = False
				pass
		else:
			bu.debug('No DB in use')

		# TBD
		if args.mode == OPT_MODE_HELP: 
			await help_extended(db, parser)
			sys.exit(0)
		
		# rebase file arguments due to moving the working directory to the script location
		args.files = bu.rebase_file_args(current_dir, args.files)

		try:
			replayQ  = asyncio.Queue(maxsize=1000)			
			reader_tasks = []
			# Make replay Queue

			scanner_task = asyncio.create_task(mk_replayQ(replayQ, args, db))
			bu.debug('Replay scanner started')
			# Start tasks to process the Queue
			for i in range(OPT_WORKERS_N):
				reader_tasks.append(asyncio.create_task(replay_reader(replayQ, i, args)))
				bu.debug('ReplayReader ' + str(i) + ' started')

			bu.debug('Waiting for the replay scanner to finish')
			await asyncio.wait([scanner_task])
			
			# bu.debug('Scanner finished. Waiting for replay readers to finish the queue')
			await replayQ.join()
			await asyncio.sleep(0.1)
			bu.debug('Replays read. Cancelling Readers and analyzing results')
			for task in reader_tasks:
				task.cancel()
				await asyncio.sleep(0.1)	
			results = []
			players = set()
			for res in await asyncio.gather(*reader_tasks):
				results.extend(res[0])
				players.update(res[1])
			if len(players) == 0:
				raise Exception("No players found to fetch stats for. No replays found?")

			(player_stats, stat_id_map) = await process_player_stats(players, OPT_WORKERS_N, args, db)
			bu.verbose('')
			bu.debug('Number of player stats: ' + str(len(player_stats)))
			teamresults = calc_team_stats(results, player_stats, stat_id_map, args)
			res = process_battle_results(teamresults, args, res_categories)	
			if args.hist: 				
				res['histograms'] = process_player_dist(results, player_stats, stat_id_map, args.json)
			if args.json:
				if args.outfile == '-':
					args.outfile = 'export.json'
					bu.verbose_std('Data exported to ' + args.outfile)
				await bu.save_JSON(args.outfile, res, pretty=False) 
			bu.debug('Finished. Cleaning up..................')
		except Exception as err:
			bu.error(exception=err)
	except UserWarning as err:
		bu.verbose(str(err))
		pass
	except Exception as err:
			bu.error(exception=err)
	finally:
		## Need to close the aiohttp.session since Python destructors do not support async methods... 
		if wg != None: await wg.close()
		if wi != None: await wi.close()
	return None


async def help_extended(db : motor.motor_asyncio.AsyncIOMotorDatabase = None, parser: argparse.ArgumentParser = None):
	"""Help for --filters"""
	if parser != None:
		parser.print_help()
	# Result category options

	# Filter usage
	print("\nSyntax: --filters '{ \"replay.param\": VALUE, \"replay.param2\": { \"$MONGO_OPERATOR\" : VALUE }, ... }'")
	print("\tExample: --filters '{ \"data.summary.protagonist\": ACCOUNT_ID, \"data.summary.battle_start_timestamp\": { \"$gt\" : 1602853200 } }'")
	if db != None:
		dbc = db[DB_C_REPLAYS]
		try:
			res = await dbc.find_one()
			summary = res['data']['summary']
			details = summary['details'][0]
			print('\n--filters KEYS:')
			for s in summary:
				print("\tdata.summary." + s)
			for d in details:
				print("\tdata.summary.details.[]." + d)
		except Exception as err:
			bu.error(exception=err)


def set_histogram_buckets(json: dict):
	global histogram_fields
	try:
		for field in json.keys():
			histogram_fields[field][1] = json[field]
	except (KeyError, Exception) as err:
		bu.error(exception=err)
	return histogram_fields


def process_player_dist(results: list, player_stats: dict, stat_id_map: dict, res_json: bool = False) -> dict:
	"""Process player distribution"""
	hist_stats = dict()
	try:
		for field in histogram_fields:
			hist_stats[field] = PlayerHistogram(field, histogram_fields[field][0], histogram_fields[field][1], histogram_fields[field][2], histogram_fields[field][3] )

		for result in results:
			for player in result['allies'] | result['enemies']:   # uniton of Sets
				player_remapped = stat_id_map[player]
				if player_remapped in player_stats:
					if player in result['allies']:
						for stat_field in hist_stats:
							hist_stats[stat_field].record_ally(player_stats[player_remapped][stat_field])
					else:
						for stat_field in hist_stats:
							hist_stats[stat_field].record_enemy(player_stats[player_remapped][stat_field])
		
		print('\nPlayer Histograms______', end='', flush=True)

		res = dict()
		for stat_field in hist_stats:
			res[stat_field] = hist_stats[stat_field].print(res_json)
		
		return res	
	except Exception as err:
		bu.error(exception=err)

	return None


def process_battle_results(results: dict, args : argparse.Namespace, cats: list):
	"""Process replay battle results""" 
	try:
		url 		= args.url
		ret_json 	= args.json
		urls 		= collections.OrderedDict()
		categories 	= {}

		for cat in cats:
			categories[cat] = BattleRecordCategory(cat)

		max_title_len = 0
		for result in results:
			for cat in cats:
				categories[cat].record_result(result)
			if url:
				max_title_len = max(max_title_len, len(result['title']))
				urls[result['title']] = result['url']
		
		res = dict()
		if ret_json:
			cat = next(iter(categories))			
			res['fields'] = categories[cat].get_fields()			
		for cat in categories:
			categories[cat].calc_results()
			print('')
			categories[cat].print_results()
			if ret_json:
				res[cat] = categories[cat].get_results_json()
		if url:
			print('\nURLs to Battle replays:\n')
			json_urls = dict()
			for title in urls.keys():
				print(('{:' + str(3 + max_title_len) + '}').format(title) + ' : ' + urls[title])
				if ret_json:
					json_urls[title] = urls[title]
			
			if ret_json:
				res['replay_urls'] = json_urls
	except Exception as err:
		bu.error(exception=err)
	return res


async def process_player_stats(players, N_workers: int, args : argparse.Namespace, db : motor.motor_asyncio.AsyncIOMotorDatabase) -> dict:
	"""Start stat_workers to retrieve and store players' stats"""
	## Create queue of player-tank pairs to find stats for
	try:
		statsQ = asyncio.Queue()
		bu.debug('Create player stats queue: ' + str(len(players)) + ' players')
		stat_id_map = {}
		stat_ids = set()

		bu.set_progress_bar('Fetching player stats', len(players), 25, True)

		stat_id_map_func = globals()[STAT_FUNC[args.stat_func][0]]

		for player in players:
			stat_id_map[player] = stat_id_map_func(player)
			stat_ids.add(stat_id_map[player])
			
		# create statsQ
		for stat_id in stat_ids:
			await statsQ.put(stat_id)

		# Process player WR / battle stats
		stats_tasks = []
		bu.debug('Starting player stats workers')
		for i in range(N_workers):
			bu.debug("Starting worker " + str(i))
			stats_tasks.append(asyncio.create_task(stat_worker(statsQ, i, args, db)))
		
		bu.debug('Waiting stats workers to finish')
		await statsQ.join()
		bu.debug('Cancelling stats workers')
		for task in stats_tasks: 
			task.cancel()	
		player_stats = {}
		stat_id_remap = {}

		bu.debug('Gathering stats worker outputs')
		for (stats, id_remap) in await asyncio.gather(*stats_tasks):
			player_stats = {**player_stats, **stats}
			stat_id_remap = {**stat_id_remap, **id_remap}

		bu.finish_progress_bar()
		
		## DO REMAPPING
		stat_id_map = remap_stat_id(stat_id_map, stat_id_remap)

		bu.debug('Returning player_stats')
		return (player_stats, stat_id_map)
	except Exception as err:
		bu.error(exception=err)
	return None


def remap_stat_id(stat_id_map: dict, stat_id_remap: dict):
	bu.debug('Remapping stat_ids: stat_id_map: ' + str(len(stat_id_map)) + ' remap needed: ' + str(len(stat_id_remap)))
	for stat_id in stat_id_map:
	 	# bu.debug(stat_id)
	 	if stat_id_map[stat_id] in stat_id_remap.keys():
	 		stat_id_map[stat_id] = stat_id_remap[stat_id_map[stat_id]]
	return stat_id_map


def calc_team_stats(results: list, player_stats  : dict, stat_id_map : dict, args : argparse.Namespace) -> list:
	"""Calculate team stats"""
	
	return_list = []

	## Bug here?? 
	#stat_types = player_stats[list(player_stats.keys())[0]].keys()
	stat_types = list()
	stat_types = BattleRecord.get_team_fields()
		
	for result in results:
		try:
			missing_stats = 0
			n_players = len(result['allies']) + len(result['enemies'])
			n_allies = collections.defaultdict(def_value_zero)
			allies_stats = collections.defaultdict(def_value_zero)
			
			#bu.debug('Processing Allies')
			for ally in result['allies']:
				# Player itself is not in 'allies': see read_replay_JSON()
				ally_mapped = stat_id_map[ally]
				if ally_mapped not in player_stats.keys(): 
					missing_stats += 1
					continue
				for stat in stat_types:
					if player_stats[ally_mapped][stat] != None:
						allies_stats[stat] += player_stats[ally_mapped][stat]
						n_allies[stat] += 1
			
			#bu.debug('Processing Enemies')
			n_enemies = collections.defaultdict(def_value_zero)
			enemies_stats = collections.defaultdict(def_value_zero)
			for enemy in result['enemies']:
				enemy_mapped = stat_id_map[enemy]
				if enemy_mapped not in player_stats: 
					missing_stats += 1
					continue
				for stat in stat_types:
					if player_stats[enemy_mapped][stat] != None:
						enemies_stats[stat] += player_stats[enemy_mapped][stat]
						n_enemies[stat] += 1
			
			#bu.debug('Processing player's own stats')
			player_mapped = stat_id_map[result['player']]
			if player_mapped not in player_stats:
				missing_stats += 1				
			else:				
				for stat in stat_types:
					if player_stats[player_mapped][stat] != None:
						result['player_' + str(stat)] = player_stats[player_mapped][stat]

			for stat in stat_types:
				if  n_allies[stat] > 0:
					result['allies_' + str(stat)] = allies_stats[stat] / n_allies[stat]
				else:
					bu.debug('No allies stats for: ' + str(result))
				
				if  n_enemies[stat] > 0:
					result['enemies_' + str(stat)] = enemies_stats[stat] / n_enemies[stat]
				else:
					bu.debug('No enemies stats for: ' + str(result))

			# Steamroller stats
			result['team_result'] = str(result['allies_survived']) + '-' + str(result['enemies_survived'])
				
			result[N_PLAYERS] = n_players
			result[MISSING_STATS] = missing_stats
			return_list.append(result)
		except KeyError as err:
			bu.error('Key not found', err) 
		except Exception as err:
			bu.error(exception=err)
	
	return return_list


async def stat_worker(queue : asyncio.Queue, workerID: int, args : argparse.Namespace, db : motor.motor_asyncio.AsyncIOMotorDatabase) -> list:
	"""Worker thread to find stats for player / tank pairs"""
	# global wg
	stats 			= {}
	stat_id_remap 	= {}

	stat_db_func = globals()[STAT_FUNC[args.stat_func][1]]
	stat_wg_func = globals()[STAT_FUNC[args.stat_func][2]]

	bu.debug("workedID: " + str(workerID) + ' started')
	try:
		while True:
			stat_id = await queue.get()
			try:
				bu.debug('Stat_id: ' + stat_id)
				bu.print_progress()
				# Analysing player performance based on their stats on the tier tanks they are playing 

				# Try cache first
				if (stat_id not in stats):
					stats_tmp = None
					pruned_stat_id = prune_stat_id(stat_id)
					if (pruned_stat_id not in stats):
						stats_tmp = await stat_wg_func(pruned_stat_id, cache_only = True)
					else:
						stat_id_remap[stat_id] = pruned_stat_id
						queue.task_done()
						continue

					if (stats_tmp != None):
						stats[pruned_stat_id] = stats_tmp
						stat_id_remap[stat_id] = pruned_stat_id
						queue.task_done()
						continue
					
					# try DB
					stats[stat_id] = await stat_db_func(db, stat_id)				
					bu.debug('get_db_' + args.stat_func + '_stats returned: ' + str(stats[stat_id]), workerID)

					# no DB stats found, trying WG AP
					if (stats[stat_id] == None):							
						stats[pruned_stat_id] = await stat_wg_func(pruned_stat_id)
						stat_id_remap[stat_id] = pruned_stat_id
						del stats[stat_id]
					
			except KeyError as err:
				bu.error('Key not found', err, workerID)
			except Exception as err:
				bu.error('Unexpected error', err, workerID)
			queue.task_done()

	except (asyncio.CancelledError, concurrent.futures._base.CancelledError):
		bu.debug('Stats queue is empty', workerID)		
	except Exception as err:
		bu.error(exception=err)
	finally:
		# remove empty stats 
		try:
			keys_2_del = []
			for key in stats:
				if stats[key] == None: 
					keys_2_del.append(key)		
			for key in keys_2_del:
				del stats[key]				
		except KeyError as err:
			bu.error('Error in pruning empty stats', err)
	# bu.debug('Returning stats & exiting')		
	return (stats, stat_id_remap)
	
## player stat functions: tank_tier
async def get_wg_tank_tier_stats(stat_id_str: str, cache_only = False) -> dict:
	"""Get player stats from WG. Returns WR per tier of tank_id"""
	try:
		(account_id, tier) = str2ints(stat_id_str)
		tier_tanks = wg.get_tanks_by_tier(tier)

		# 'battles' must always be there
		hist_stats = [ 'all.' + x for x in histogram_fields.keys() ]
		hist_stats.append('tank_id')

		player_stats = await wg.get_player_tank_stats(account_id, tier_tanks, hist_stats, cache_only = cache_only)
		#bu.debug('account_id: ' + str(account_id) + ' ' + str(player_stats))

		return await tank_stats_helper(player_stats)

	except KeyError as err:
		bu.error('account_id: ' + str(account_id) + ' Key not found', err)
	except Exception as err:
		bu.error(exception=err)
	return None
	

async def get_db_tank_tier_stats(db : motor.motor_asyncio.AsyncIOMotorDatabase, stat_id_str: str) -> dict:
	"""Get player stats from MongoDB (you are very unlikely to have it, unless you have set it up)"""
	if db == None:
		return None
	try:
		dbc = db[DB_C_TANK_STATS]
		( account_id, tier, battletime ) = str2ints(stat_id_str)
		tier_tanks = wg.get_tanks_by_tier(tier)
		time_buffer = 2*7*24*3600
		#bu.debug('Tank_ids: ' + ','.join(map(str, tier_tanks)))
		
		# if battletime == None:
		# 	pipeline = 	[ { '$match': { '$and': [ { 'account_id': account_id }, { 'tank_id' : {'$in': tier_tanks} } ]}}, 
		# 			{ '$sort': { 'last_battle_time': -1 }}, 
		# 			{ '$group': { '_id': '$tank_id', 'doc': { '$first': '$$ROOT' }}}, 
		# 			{ '$replaceRoot': { 'newRoot': '$doc' }}, 
		# 			{ '$project': { '_id': 0 }} ]
		# else:
		pipeline = 	[ { '$match': { '$and': [ { 'account_id': account_id }, { 'last_battle_time': { '$lte': battletime + time_buffer }}, { 'tank_id' : {'$in': tier_tanks} } ]}}, 
				{ '$sort': { 'last_battle_time': -1 }}, 
				{ '$group': { '_id': '$tank_id', 'doc': { '$first': '$$ROOT' }}}, 
				{ '$replaceRoot': { 'newRoot': '$doc' }}, 
				{ '$project': { '_id': 0 }} ]

		# cursor = dbc.aggregate(pipeline, allowDiskUse=True)
		cursor = dbc.aggregate(pipeline)

		return await tank_stats_helper(await cursor.to_list(1000)) 
				
	except Exception as err:
		bu.error('account_id: ' + str(account_id) + ' Error', err)
	return None


## player stat functions: player
async def get_db_player_stats(db : motor.motor_asyncio.AsyncIOMotorDatabase, stat_id_str: str) -> dict:
	"""Get player stats from MongoDB (you are very unlikely to have it, unless you have set it up)"""
	if db == None:
		return None
	try:
		dbc = db[DB_C_TANK_STATS]  ## not collecting player stats yet 2019-12-23
		( account_id, battletime ) = str2ints(stat_id_str)
		time_buffer = 2*7*24*3600
		
		pipeline = 	[ { '$match': { '$and': [ { 'account_id': account_id }, { 'last_battle_time': { '$lte': battletime + time_buffer }} ]}}, 
				{ '$sort': { 'last_battle_time': -1 }}, 
				{ '$group': { '_id': '$tank_id', 'doc': { '$first': '$$ROOT' }}}, 
				{ '$replaceRoot': { 'newRoot': '$doc' }}, 
				{ '$project': { '_id': 0 }} ]

		# cursor = dbc.aggregate(pipeline, allowDiskUse=True)
		cursor = dbc.aggregate(pipeline)

		return await tank_stats_helper(await cursor.to_list(1000)) 
				
	except Exception as err:
		bu.error('account_id: ' + str(account_id) + ' Error', err)
	return None


async def get_wg_player_stats(stat_id_str: str, cache_only = False) -> dict:
	"""Get player stats from WG. Returns WR per tier of tank_id"""
	try:
		account_id = int(stat_id_str)
		
		# 'battles' must always be there
		hist_stats = [ 'statistics.all.' + x for x in histogram_fields.keys() ]
		hist_stats.append('account_id')

		player_stats = await wg.get_player_stats(account_id, hist_stats, cache_only = cache_only)
		#bu.debug('account_id: ' + str(account_id) + ' ' + str(player_stats))

		return await player_stats_helper(player_stats)

	except KeyError as err:
		bu.error('account_id: ' + str(account_id) + ' Key not found', err)
	except Exception as err:
		bu.error(exception=err)
	return None


async def tank_stats_helper(stat_list: list):
	"""Helpher func got get_db_tank_tier_stats() and get_wg_tank_tier_stats()"""
	try:
		if stat_list == None: 
			return None
		stats = {}
		hist_fields = histogram_fields.keys()
		if 'battles' not in hist_fields:
			bu.error('\'battles\' must be defined in \'histogram_fields\'')
			return None

		for field in hist_fields:
			stats[field] = 0
		
		for tank_stat in stat_list: 
			tank_stat = tank_stat['all']
			for field in hist_fields:
				stats[field] += max(0, tank_stat[field])

		if stats['battles'] == 0:
			return None

		for field in hist_fields:
			if field == 'battles': continue
			stats[field] = stats[field] / stats['battles']
		return stats

	except Exception as err:
		bu.error(exception=err)
	return None


async def player_stats_helper(player_stats: dict):
	"""Extract relevant stats for player histogram from raw player stats"""
	try:
		if player_stats == None: 
			return None
		stats = {}
		hist_fields = histogram_fields.keys()
		if 'battles' not in hist_fields:
			bu.error('\'battles\' must be defined in \'histogram_fields\'')
			return None

		# NEEDED 2020-10-25? 
		#for field in hist_fields:
		#	stats[field] = 0
		
		for field in hist_fields:
			stats[field] = max(0, player_stats['statistics']['all'][field])

		if stats['battles'] == 0:
			return None

		for field in hist_fields:
			if field == 'battles': continue
			stats[field] = stats[field] / stats['battles']
		return stats

	except Exception as err:
		bu.error(exception=err)
	return None


async def mk_replayQ(queue : asyncio.Queue, args : argparse.Namespace, db : motor.motor_asyncio.AsyncIOMotorDatabase = None):
	"""Create queue of replays to post"""
	p_replayfile = re.compile('.*\\.wotbreplay\\.json$')
	files = args.files
	Nreplays = 0
	if files[0] == 'db:':
		if db == None:
			bu.error('No database connection opened')
			sys.exit(1)
		try:
			dbc = db[DB_C_REPLAYS]
			cursor = None
			if args.filters  != None:
				bu.debug(str(args.filters))
				filters = json.loads(args.filters)
				bu.debug(json.dumps(filters, indent=2))
				cursor = dbc.find(filters)
			else:
				# select all
				cursor = dbc.find({})
			bu.debug('Reading replays...')	
			async for replay_json in cursor:
				_id = replay_json['_id']
				del(replay_json['_id'])
				await queue.put(await mk_readerQ_item(replay_json, 'DB: _id = ' + _id))
				Nreplays += 1
			bu.debug('All the matching replays have been read from the DB')
		except Exception as err:
			bu.error(exception=err)

	elif files[0] == '-':
		bu.debug('reading replay file list from STDIN')
		stdin, _ = await aioconsole.get_standard_streams()
		while True:
			try:
				line = (await stdin.readline()).decode('utf-8').rstrip()
				if not line: break
				
				if (p_replayfile.match(line) != None):
					replay_json = await bu.open_JSON(line, wi.chk_JSON_replay)			
					await queue.put(await mk_readerQ_item(replay_json, line))
			except Exception as err:
				bu.error(exception=err)
	else:
		for fn in files:
			try:
				# bu.debug('Filename: ' + fn)
				if fn.endswith('"'):
					fn = fn[:-1] 
				if os.path.isfile(fn) and (p_replayfile.match(fn) != None):
					replay_json = await bu.open_JSON(fn, wi.chk_JSON_replay)
					await queue.put(await mk_readerQ_item(replay_json, fn))
					bu.debug('File added to queue: ' + fn)
					Nreplays += 1
				elif os.path.isdir(fn):
					with os.scandir(fn) as dirEntry:
						for entry in dirEntry:
							if entry.is_file() and (p_replayfile.match(entry.name) != None): 
								bu.debug(entry.name)
								replay_json = await bu.open_JSON(entry.path, wi.chk_JSON_replay)
								await queue.put(await mk_readerQ_item(replay_json, entry.name))
								bu.debug('File added to queue: ' + entry.path)
								Nreplays += 1
			except Exception as err:
				bu.error(exception=err)					
	bu.verbose('Finished scanning replays: ' + str(Nreplays)  + ' replays to process') 
	return Nreplays


async def mk_readerQ_item(replay_json, filename : str = None) -> list:
	"""Make an item to replay queue"""
	global REPLAY_N
	REPLAY_N +=1	
	if filename == None:
		return [replay_json, REPLAY_N, '' ]
	elif RE_SRC_IS_DB.match(filename) != None:
		return [replay_json, REPLAY_N, filename ]
	else:
		return [replay_json, REPLAY_N, os.path.basename(filename) ]


async def replay_reader(queue: asyncio.Queue, readerID: int, args : argparse.Namespace):
	"""Async Worker to process the replay queue"""
	#global SKIPPED_N
	results = []
	playerstanks = set()
	try:
		while True:
			item = await queue.get()
			replay_json 	= item[0]
			replayID 		= item[1]
			replay_file 	= item[2]

			try:
				msg_str = 'Replay[' + str(replayID) + ']: ' 
				if replay_json == None:
					bu.warning(msg_str + 'Invalid replay. Skipping: '  + (replay_file if replay_file != None else '') )
					#SKIPPED_N += 1
					queue.task_done()
					continue
						
				## Read the replay JSON
				bu.debug('reading replay', readerID)
				result = await read_replay_JSON(replay_json, args)
				bu.print_progress()
				if result == None:
					bu.warning(msg_str + 'Invalid replay' + (replay_file if replay_file != None else '') )
					queue.task_done()
					continue
				
				# if (account_id != None):						
				playerstanks.update(set(result['allies']))
				playerstanks.update(set(result['enemies']))	
				playerstanks.update(set([ result['player'] ]))
				
				results.append(result)
				bu.debug('Marking task ' + str(replayID) + ' done')
						
			except Exception as err:
				bu.error(exception=err)
			queue.task_done()
	except (asyncio.CancelledError, concurrent.futures.CancelledError):		
		bu.debug( str(len(results)) + ' replays, ' + str(len(playerstanks)) + ' player/tanks', readerID)
		return results, playerstanks
	return None


async def read_replay_JSON(replay_json: dict, args : argparse.Namespace) -> dict:
	"""Parse replay JSON dict"""
	global REPLAY_I
	account_id = args.account_id
	url = args.url
	#db = args.db
	result = {}
	try:
		# bu.debug(str(replay_json))
		if not wi.chk_JSON_replay(replay_json):
			bu.debug('Invalid replay')
			return None
		
		result['battle_start_timestamp'] = int(replay_json['data']['summary']['battle_start_timestamp'])
		# TBD... 
		protagonist = int(replay_json['data']['summary']['protagonist'])
		
		if account_id == None:
			account_id = protagonist
		elif protagonist != account_id:
			if account_id in replay_json['data']['summary']['enemies']:
				# switch the teams...
				if replay_json['data']['summary']['battle_result'] != 2:
					if replay_json['data']['summary']['battle_result'] == 0:
						replay_json['data']['summary']['battle_result'] = 1
					else: 
						replay_json['data']['summary']['battle_result'] = 0
				tmp = replay_json['data']['summary']['enemies']
				replay_json['data']['summary']['enemies'] = replay_json['data']['summary']['allies']
				replay_json['data']['summary']['allies'] = tmp
			elif account_id not in replay_json['data']['summary']['allies']:
				# account_id looked for but not found in teams
				bu.debug('Replay ' + replay_json['data']['summary']['title'] + ' does not have account_id ' + str(account_id) + '. Skipping.')
				account_id = protagonist
				# return None
		if url: 
			result['url'] = replay_json['data']['view_url']
				
		for key in replay_summary_flds:
			result[key] = replay_json['data']['summary'][key]
	
	except Exception as err:
		bu.error(exception=err)
		return None
	try:	
		bu.debug('Part 2')
		result['allies'] = set()
		result['enemies'] = set()
		result['allies_survived']  = 0 	# for steamroller stats
		result['enemies_survived']  = 0	# for steamroller stats
		btl_duration = 0
		btl_tier = 0
		protagonist_tank  = None
		for player in replay_json['data']['summary']['details']:
			btl_duration = max(btl_duration, player['time_alive'])
			player_tank_tier = wg.get_tank_data(player['vehicle_descr'], 'tier')
			btl_tier = max(btl_tier, player_tank_tier)

			if (protagonist != None) and (player['dbid'] == protagonist):
				protagonist_tank = player['vehicle_descr']

			if player['dbid'] == account_id:
				# player itself is not put in results['allies']
				tmp = {}
				tmp['account_id'] 	= account_id
				tmp['tank_id'] 		= player['vehicle_descr']
				tmp['tank_tier'] 	= player_tank_tier
				tmp['tank_name']	= wg.get_tank_data(tmp['tank_id'], 'name')
				tmp['squad_index'] 	= player['squad_index']
				for key in replay_details_flds:
					tmp[key] = player[key]
				if player['hitpoints_left'] == 0:
					tmp['survived'] = 0
					tmp['destroyed'] = 1
				else:
					tmp['survived'] 	= 1
					tmp['destroyed'] 	= 0
					result['allies_survived'] += 1
				for key in tmp.keys():
					result[key] = tmp[key]				

			else:
				tmp_account_id 	= player['dbid']
				tmp_tank_id 	= player['vehicle_descr']
				tmp_battletime	= result['battle_start_timestamp']
				if player['death_reason'] == -1:
					survived = True
				else:
					survived = False
				
				if player['dbid'] in replay_json['data']['summary']['allies']: 
					result['allies'].add(get_stat_id(tmp_account_id, tmp_tank_id, tmp_battletime))
					if survived:
						result['allies_survived'] += 1
				else:
					result['enemies'].add(get_stat_id(tmp_account_id, tmp_tank_id, tmp_battletime))
					if survived:
						result['enemies_survived'] += 1

		## Rather use 'player' than incomprehensible 'protagonist'...		
		result['player'] = get_stat_id(protagonist, protagonist_tank, result['battle_start_timestamp'])
		bu.debug('Player stats_id: ' + result['player'])
		# remove platoon buddy from stats 			
		if result['squad_index'] != None:
			for player in replay_json['data']['summary']['details']:
				bu.debug(str(player))
				if (player['squad_index'] == result['squad_index']) and (player['dbid'] in replay_json['data']['summary']['allies']) and (player['dbid'] != account_id):
					# platoon buddy found 
					tmp_account_id 	= player['dbid']
					tmp_tank_id 	= player['vehicle_descr']
					#tmp_tank_tier 	= str(wg.get_tank_tier(tmp_tank_id))
					tmp_battletime	= result['battle_start_timestamp']
					# platoon buddy removed from stats 
					result['allies'].remove(get_stat_id(tmp_account_id, tmp_tank_id, tmp_battletime))
					break

		result['time_alive%'] = result['time_alive'] / btl_duration  
		result['battle_tier'] = btl_tier
		result['top_tier'] = 1 if (result['tank_tier'] == btl_tier) else 0
		result['win'] = 1 if result['battle_result'] == 1 else 0
		REPLAY_I += 1
		result['battle_i'] = REPLAY_I
		bu.debug(str(result))
		return result
	except KeyError as err:
		bu.error('Key not found', err)
	except Exception as err:
		bu.error(exception=err)
	return None


def str2ints(stat_id_str: str) -> list:
	"""Convert stat_id string e.g. 'account_id:tank_id' to list of ints""" 
	return [ int(x) for x in stat_id_str.split(':') ]


def prune_stat_id(stat_id_str: str) -> str: 
	stat_id 		= stat_id_str.split(':')	
	del stat_id[-1]
	return ':'.join(stat_id)


def get_stat_id(account_id: int, tank_id: int, battletime: int) -> str:
	return ':'.join([ str(account_id), str(tank_id), str(battletime)])


def get_stat_id_tank_tier(stat_id_str: str) -> str:
	"""Return stat_id = account_id:tank_tier"""
	try:
		stat_id 	= stat_id_str.split(':')
		tank_tier = wg.get_tank_tier(int(stat_id[1]))
		battle_time = (int(stat_id[2]) // BATTLE_TIME_BUCKET) * BATTLE_TIME_BUCKET
		return ':'.join([ stat_id[0], str(tank_tier), str(battle_time) ])
	except Exception as err:
		bu.error('Stats_id: ' + stat_id_str)
		bu.error(exception=err)
	return None


def get_stat_id_player(stat_id_str: str) -> str:
	"""Return stat_id = account_id:tank_tier"""
	try:
		stat_id 	= stat_id_str.split(':')
		battle_time = (int(stat_id[2]) // BATTLE_TIME_BUCKET) * BATTLE_TIME_BUCKET
		return ':'.join([ stat_id[0], str(battle_time) ])
	except Exception as err:
		bu.error('Stats_id: ' + stat_id_str)
		bu.error(exception=err)
	return None


### main()
if __name__ == "__main__":
	# To avoid 'Event loop is closed' RuntimeError due to compatibility issue with aiohttp
	if sys.platform.startswith("win") and sys.version_info >= (3, 8):
		try:
			from asyncio import WindowsSelectorEventLoopPolicy
		except ImportError:
			pass
		else:
			if not isinstance(asyncio.get_event_loop_policy(), WindowsSelectorEventLoopPolicy):
				asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
	asyncio.run(main(sys.argv[1:]), debug=False)
