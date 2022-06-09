#!/usr/bin/env python3
## PYTHON VERSION MUST BE 3.7 OR HIGHER

# Script Analyze WoT Blitz replays

import sys, argparse, json, os, concurrent, inspect, aiohttp, asyncio, aiofiles, aioconsole
import motor.motor_asyncio, configparser, ssl, re, logging, time, xmltodict, collections, csv
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

OPT_EXPORT_REPLAY_DIR = 'export_replays'
OPT_EXPORT_CSV_FILE	 = 'export.csv'
OPT_EXPORT_JSON_FILE = 'export.json'

RE_SRC_IS_DB = re.compile(r'^DB:')

class StatFunc:
## For different player stat functions (global stats, tank tier stats, etc)
# 1st function = for forming stat_id, 2nd for DB stats query, 3rd for WG API stats query
	STAT_FUNC	= {
		'tank_tier': 	[ 'get_stat_id_tank_tier', 'get_db_tank_tier_stats', 'get_wg_tank_tier_stats', 'WR at tier' ],
		'tank': 		[ 'get_stat_id_tank', 'get_db_tank_stats', 'get_wg_tank_stats', 'WR in Tank' ],
		'player': 		[ 'get_stat_id_player', 'get_db_player_stats', 'get_wg_player_stats', 'Career WR' ], 
		'tier_x':		[ 'get_stat_id_player', 'get_db_tier_x_stats', 'get_wg_tier_x_stats', 'WR at tier X' ],
	}
	
	_DEFAULT = 'player'

	_stat_func = _DEFAULT

	@classmethod
	def get_default(cls):
		return cls._DEFAULT


	@classmethod
	def get_stat_funcs(cls):
		return cls.STAT_FUNC.keys()


	@classmethod
	def set_stat_func(cls, stat_func: str = _DEFAULT):
		if stat_func in cls.STAT_FUNC.keys():
			cls._stat_func = stat_func
		else:
			## ERROR
			pass
		return cls._stat_func


	@classmethod
	def get_stat_func(cls):
		return cls._stat_func


	@classmethod
	def get_stat_id_func(cls):
		return cls.STAT_FUNC[cls._stat_func][0]


	@classmethod
	def get_db_func(cls):
		return cls.STAT_FUNC[cls._stat_func][1]

	
	@classmethod
	def get_wg_func(cls):
		return cls.STAT_FUNC[cls._stat_func][2]

		
	@classmethod
	def get_title(cls):
		return cls.STAT_FUNC[cls._stat_func][3]


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
	'wins'				: [ 'Win rate', 	[0, .35, .40, .45, .48, .5, .52, .55, .60, .65, .70, 1], 1, '{:2.0%}' ],
	'damage_dealt'		: [ 'Avg. Dmg.', 	[0, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 100e3], 1, '{:3.0f}' ],
	'battles'			: [ 'Battles', 		[0, 1000, 2500, 5000, 7000, 10e3, 15e3, 25e3, 50e3, 5e7], .001, '{:.0f}k']	# battles is a mandatory stat to include
	}


def def_value_zero():
    return 0

def def_value_BattleCategory():
	return BattleCategory()

class BattleCategorizationList():
	
	@staticmethod
	def mk_battle_modes(modes: dict):
		"""Make a list of battle modes as required for the _categorizations"""
		mode_ids = modes.values()
		id_max = max(mode_ids)
		ret_modes = ['-'] * (id_max+16)
		for i in range(0, id_max+16):
			ret_modes[i] = 'room_type=' + str(i)
				
		for mode in modes:
			id = modes[mode]
			ret_modes[id] = mode
		
		return ret_modes

	_battle_modes = {
		'Any' 					: 0, 
		'Regular'				: 1,
		'Training Room'			: 2,
		'Tournament'			: 4,
		'Quick Tournament'		: 5,  # maybe? 
		'Rating'				: 7,
		'Mad Games'				: 8,
		'Realistic Battles'		: 22,
		'Uprising'				: 23, 
		'Gravity Mode'			: 24,
		'Skirmish'				: 25,
		'Burning Games'			: 26
	}

	_BATTLE_MODES = mk_battle_modes.__func__(_battle_modes)

	_categorizations = {
		'total'					: [ 'TOTAL', 			'total' ],
		'battle_result'			: [ 'Result', 			'category', 	[ 'Loss', 'Win', 'Draw']],
		'battle_type'			: [ 'Battle Type', 		'category', 	[ 'Encounter', 'Supremacy']],
		'room_type'				: [ 'Battle Mode', 		'category', 	_BATTLE_MODES ],
		'top_tier'				: [ 'Tier', 			'category', 	[ 'Bottom tier', 'Top tier']],
		'in_platoon'			: [ 'Platoon', 			'category', 	[ 'Solo', 'Platoon']],
		'tank_tier'				: [ 'Tank Tier', 		'number' ],
		'is_premium'			: [ 'Tank Type', 		'category', 	[ 'Tech tree', 'Premium']],
		'tank_type'				: [ 'Tank Class', 		'category', 	WG.TANK_TYPE_STR],
		'tank_nation'			: [ 'Nation', 			'category', 	WG.NATION_STR],		
		'mastery_badge'			: [ 'Battle Medal', 	'category', 	['-', '3rd Class', '2nd Class', '1st Class', 'Mastery' ]],
		'team_result'			: [ 'Team Result', 		'string' ],	
		'player_wins'			: [ 'Player WR', 		'bucket', [ 0, .35, .45, .50, .55, .65], '%' ],
		'player_battles'		: [ 'Player Battles',	'bucket', [ 0, 500, 1000, 2500, 5e3, 10e3, 15e3, 25e3], 'int' ],
		'player_damage_dealt'	: [ 'Player Avg Dmg',	'bucket', [ 0, 500, 1000, 1250, 1500, 1750, 2000, 2500], 'int' ],
		'damage_made'			: [ 'Player Dmg made',	'bucket', [ 0, 500, 1000, 1250, 1500, 1750, 2000, 2500], 'int' ],
		'enemies_destroyed'		: [ 'Player Kills', 	'number'],
		'enemies_spotted'		: [ 'Player Spots',  	'number'],
		'hit_rate'				: [ 'Player Hit Rate', 	'bucket', [ 0, .25, .5, .6, .7, .8, .9, .95 ], '%' ],
		'pen_rate'				: [ 'Player Pen Rate', 	'bucket', [ 0, .25, .5, .6, .7, .8, .9, .95 ], '%' ],
		'alive'					: [ 'Time alive (pcs)', 'bucket', [ x/100 for x in range(0,100,10) ], '%' ],
		'time_alive'			: [ 'Time alive (s)',	'bucket', [ x*60 for x in range(0,8) ], 'int' ],
		'battle_duration'		: [ 'Battle Duration', 	'bucket', [ x*60 for x in range(0,8) ], 'int' ],
		'distance_travelled'	: [ 'Distance Driven',	'bucket', [ x*250 for x in range(0,20) ], 'int' ],
		'allies_wins'			: [ 'Allies WR', 		'bucket', [ 0, .35, .45, .50, .55, .65], '%' ],
		'allies_battles'		: [ 'Player Battles',	'bucket', [ 0, 500, 1000, 2500, 5e3, 10e3, 15e3, 25e3], 'int' ],
		'allies_damage_dealt'	: [ 'Player Avg Dmg',	'bucket', [ 0, 500, 1000, 1250, 1500, 1750, 2000, 2500], 'int' ],
		'enemies_wins'			: [ 'Enemies WR', 		'bucket', [ 0, .35, .45, .50, .55, .65], '%' ],
		'enemies_battles'		: [ 'Player Battles',	'bucket', [ 0, 500, 1000, 2500, 5e3, 10e3, 15e3, 25e3], 'int' ],
		'enemies_damage_dealt'	: [ 'Player Avg Dmg',	'bucket', [ 0, 500, 1000, 1250, 1500, 1750, 2000, 2500], 'int' ],
		'player_name'			: [ 'Player', 			'string', 25 ],
		'protagonist'			: [ 'account_id', 		'number' ],
		'tank_name'				: [ 'Tank', 			'string', 25],
		'map_name'				: [ 'Map', 				'string', 20],	
		'battle'				: [ 'Battle', 			'string', 40],
		'battle_i'				: [ 'Battle #', 		'number' ]		
		}

	_categorizations_default = [
		'total',
		'battle_result',
		'battle_type',
		'tank_tier', 
		'top_tier'
		]

	## Player stats based categorizations
	_categorizations_stats = [ group + '_' + measure for group in [ 'player', 'allies', 'enemies'] for measure in ['wins', 'battles', 'damage_dealt' ]]

	
	@classmethod
	def get_categorizations_all(cls, cats: list = None) -> list:
		try:
			if cats == None:
				return list(cls._categorizations.keys())
			else:
				return list(set(cls._categorizations.keys()) & set(cats)) 
		except Exception as err:
			bu.error(exception=err)
		return None	


	@classmethod
	def get_categorizations_stats(cls, cats=None, stats_cats=None):
		try:
			if cats == None:
				return cls._categorizations_stats
			else:
				if stats_cats == None:
					return cats
				player_stat_filters = set(cls._categorizations_stats)
				cats = set(cls.get_categorizations_all(cats))
				if stats_cats:
					cats = cats & player_stat_filters
				else:
					cats = cats - player_stat_filters
				bu.debug(', '.join(list(cats)))
				return list(cats)
		except Exception as err:
			bu.error(exception=err)
		return None		


	@classmethod
	def get_categorizations_default(cls):
		return cls._categorizations_default


	@classmethod
	def set_categorizations_default(cls, cat_defaults: list):
		try:
			defaults = list()
			for cat in cat_defaults:
				if cat in cls.get_categorizations_all():
					defaults.append(cat)
				else:
					bu.error(cat + ' is not a defined categorization')
			cls._categorizations_default = defaults
		except Exception as err:
			bu.error(exception=err)


	@classmethod
	def get_categorizations_extra(cls) -> list:
		#return cls._categorizations_extra
		return list( set(cls._categorizations.keys() ) - set(cls.get_categorizations_default() ) )


	@classmethod
	def get_categorization_title(cls, cat: str) -> str:
		try:
			return cls._categorizations[cat][0]
		except Exception as err:
			bu.error(exception=err)
		return None


	@classmethod
	def get_categorization_type(cls, cat: str) -> str:
		try:
			return cls._categorizations[cat][1]
		except Exception as err:
			bu.error(exception=err)
		return None


	@classmethod
	def get_categorization_params(cls, cat: str) -> list:
		try:
			if len(cls._categorizations[cat]) > 2:
				return cls._categorizations[cat][2:]
		except Exception as err:
			bu.error(exception=err)
		return None
	

	@classmethod
	def help(cls):
		for cat in cls.get_categorizations_all():
			try:
				ctitle 	= cls.get_categorization_title(cat)
				ctype 	= cls.get_categorization_type(cat)
				cparams = cls.get_categorization_params(cat)
						
				if ctype == 'number':
					cat_tmp = BattleNumberCategorization(cat, ctitle)
				elif ctype == 'string':
					cat_tmp = BattleStringCategorization(cat, ctitle, cparams)
				elif ctype == 'category':					
					cat_tmp = BattleClassCategorization(cat, ctitle, cparams[0])
				elif ctype == 'bucket':
					cat_tmp = BattleBucketCategorization(cat, ctitle, cparams[0], cparams[1])
				else:
					cat_tmp = BattleTotals(ctitle)
				
				cat_tmp.help()

			except KeyError as err:
				bu.error('BattleCategorizationList(): Key not found: Category: ' + cat, exception=err)			
			except Exception as err:
				bu.error('BattleCategorizationList()', exception=err) 


	@classmethod
	def get_categorizations(cls, args : argparse.Namespace):
		extra_cats = args.extra
		only_extra = args.only_extra
		
		if only_extra:
			cats = list()
		else:
			cats = cls.get_categorizations_default()
		
		if extra_cats != None: 
			cats = cats + extra_cats
		return cats


	def __init__(self, cats: list):
		self.urls 				= collections.OrderedDict()
		self.url_title_max_len 	= 0		
		
		cats = list(set(cats))  	# remove duplicates
		# ordering		
		self.categorizations_list = [ cat for cat in self._categorizations if cat in cats ]
		
		self.categorizations = dict()
		for cat in self.categorizations_list:
			try:
				ctitle 	= self.get_categorization_title(cat)
				ctype 	= self.get_categorization_type(cat)
				cparams = self.get_categorization_params(cat)
						
				if ctype == 'total':
					self.categorizations[cat] = BattleTotals(ctitle)
				elif ctype == 'number':
					self.categorizations[cat] = BattleNumberCategorization(cat, ctitle)
				elif ctype == 'string':
					self.categorizations[cat] = BattleStringCategorization(cat, ctitle, cparams)
				elif ctype == 'category':					
					self.categorizations[cat] = BattleClassCategorization(cat, ctitle, cparams[0])
				elif ctype == 'bucket':
					self.categorizations[cat] = BattleBucketCategorization(cat, ctitle, cparams[0], cparams[1])

			except KeyError as err:
				bu.error('BattleCategorizationList(): Key not found: Category: ' + cat, exception=err)			
			except Exception as err:
				bu.error('BattleCategorizationList()', exception=err) 


	def __iter__(self):
		"""Returns the Iterator object"""
		return BattleCategorizationListIterator(self)


	def len(self):
		return len(self.categorizations)


	def get_categorization(self, cat: str):	
		"""Return a BattleCategorization for a category"""
		try:
			return self.categorizations[cat]
		except Exception as err:
			bu.error(exception=err)
		return None
	

	def record_result(self, result: dict):
		try:
			for cat in self.categorizations_list:
				self.categorizations[cat].record_result(result)
		except Exception as err:
			bu.error(exception=err)
		return None


	def calc_results(self):
		try:
			for cat in self.categorizations_list:
				self.categorizations[cat].calc_results()
		except Exception as err:
			bu.error(exception=err)
		return None


	def record_url(self, result):
		try:
			self.url_title_max_len = max(self.url_title_max_len, len(result['title']))
			self.urls[result['title']] =  result['url']
		except Exception as err:
			bu.error(exception=err)
		return None


	def print_results(self):
		try:
			for cat in self.categorizations_list:
				self.categorizations[cat].print_results()
			self.print_urls()

		except Exception as err:
			bu.error(exception=err)	


	def print_urls(self):
		try:
			if len(self.urls) > 0:
				print('\nURLs to Battle replays:')
				for title in self.urls:
					print(('{:' + str(3 + self.url_title_max_len) + '}').format(title) + ' : ' + self.urls[title])
		except Exception as err:
			bu.error(exception=err)		


	def get_filter_categories(self, cat: str, filters: list) -> dict:
		try:
			return self.categorizations[str(cat)].get_filter_categories(filters)
		except Exception as err:
			bu.error(exception=err)
		return None


	def get_categories(self, result: dict) -> dict:
		"""Get categories for each categorization in the CategorizationList() for a battle result"""	
		try:
			res = dict()
			for cat in self.categorizations_list:
				res[cat] = self.categorizations[cat].get_category(result)
			return res
		except Exception as err:
			bu.error(exception=err)
		return None


	def get_results_json(self):
		try:
			res = dict()
			for cat in self.categorizations_list:
				res[cat] = self.categorizations[cat].get_results_json()
			res['replay_urls'] = self.urls
			return res
		except Exception as err:
			bu.error(exception=err)
		return None


	def get_results_list(self, urls: bool = False):
		try:
			res = list()			
			for cat in self.categorizations_list:
				res.append('')
				res.extend(self.categorizations[cat].get_results_list())			
			if urls:
				res.append('')
				for url in self.urls:
					res.append(url)
			return res
		except Exception as err:
			bu.error(exception=err)
		return None


class BattleCategorizationListIterator():
	def __init__(self, btl_cats: BattleCategorizationList) -> None:
		self._btl_cats = btl_cats
		self._ndx = 0

	def __next__(self):
		if self._ndx < len(self._btl_cats.categorizations_list):
			cat = self._btl_cats.categorizations_list[self._ndx]
			res = self._btl_cats.categorizations[cat]
			self._ndx += 1
			return res
		raise StopIteration


class BattleCategorization():

	RESULT_CAT_HEADER_LEN = 15
	RESULT_CAT_HEADER_FRMT = '{:_<' + str(RESULT_CAT_HEADER_LEN) + 's}'
	RESULT_CAT_FRMT = '{:>' + str(RESULT_CAT_HEADER_LEN) + 's}'
	
	def __init__(self, cat_key : str, title: str):
		self.total_battles 	= 0
		self.category_key 	= cat_key
		self.title 			= title
		self.categories 	= collections.defaultdict(def_value_BattleCategory)


	def set_cat_header_format(self, header_len: int, left_aligned: bool = False):
		self.RESULT_CAT_HEADER_LEN = header_len
		self.RESULT_CAT_HEADER_FRMT = '{:_<' + str(header_len) + 's}'
		if left_aligned:
			self.RESULT_CAT_FRMT = '{:<' + str(header_len) + 's}'
		else:
			self.RESULT_CAT_FRMT = '{:>' + str(header_len) + 's}'
		

	def get_categories(self):
		"""Get category keys sorted"""
		return sorted(self.categories.keys() , key=str.casefold)


	### PER Child Class
	def get_category(self, result: dict) -> str: 
		"""Get category. Must be implemented in the child classes"""
		return 'NONE'


	def get_filter_categories(self, filters: list) -> list:
		try:
			if type(filters) != list:
				filters = [ filters ]
			return [ x for x in filters if x in self.get_categories() ]
		except Exception as err:
			bu.error('Category: ' + str(self.category_key), exception=err)
		return None

	def help(self): 
		"""Help"""
		FORMAT = '\t{:<15s}\t{}. Options:'
		print(FORMAT.format(self.category_key, self.title), end=' ')		


	def record_result(self, result: dict) -> bool:
		try:
			cat = self.get_category(result)
			self.categories[cat].record_result(result)
			self.total_battles += 1
			return True
		except KeyError as err:
			bu.error('Category key not found: ' + cat, exception=err)
		except Exception as err:
			bu.error('Category key: ' + cat, exception = err) 
		return False
	

	def calc_results(self):
		try:
			for cat in self.get_categories():
				self.categories[cat].calc_results(self.total_battles)
		except Exception as err:
			bu.error(exception = err)


	def print_headers(self):
		try:
			if len(self.title) > self.RESULT_CAT_HEADER_LEN:
				cat_name = self.title[:self.RESULT_CAT_HEADER_LEN]
			else:
				cat_name = self.title
			header = self.RESULT_CAT_HEADER_FRMT.format(cat_name) 
			BattleCategory.print_headers(header)
			return None
		except Exception as err:
			bu.error(exception = err)


	def get_headers(self) -> list:
		"""Get BattleCategory headers as list"""
		try:
			return BattleCategory.get_headers(self.title)		
		except Exception as err:
			bu.error(exception = err)
		return None


	def print_results(self):
		try:			
			bu.print_new_line()
			self.print_headers()
			for cat in self.get_categories():
				if len(cat) > self.RESULT_CAT_HEADER_LEN:
					cat_name = cat[:self.RESULT_CAT_HEADER_LEN]
				else:
					cat_name = cat
				print(self.RESULT_CAT_FRMT.format(cat_name), end='   ')
				self.categories[cat].print_results()
		except KeyError as err:
			bu.error('Key not found', err)  
		except Exception as err:
			bu.error(exception = err) 
		return None


	def get_results_json(self) -> dict:
		"""Get results in JSON format"""
		try:
			results = dict()
			results['categorization'] 	= self.title
			results['category_key'] 	= self.category_key			
			for cat in self.get_categories():
				results[cat] = self.categories[cat].get_results()			

			return results
		except KeyError as err:
			bu.error('Key not found', err)  
		return None

	
	def get_results_list(self) -> list:
		"""Get results in  list format (for CSV printing)"""
		try:
			results = list()
			results.append(self.get_headers())
			for cat in self.get_categories():
				results.append( [cat] + self.categories[cat].get_results_list() )
			return results
		except KeyError as err:
			bu.error('Key not found', err)  
		return None


class BattleTotals(BattleCategorization):
	TOTAL = 'Total'

	def __init__(self, title: str):
		super().__init__(self.TOTAL, title)


	def get_category(self, result: dict) -> str: 
		return self.TOTAL


	def get_categories(self) -> str: 
		return [ self.TOTAL ]


	def help(self):
		super().help()
		print('-')


class BattleClassCategorization(BattleCategorization):
	"""Class for categorization by fixed classes with ID (int)"""
	
	def __init__(self, cat_key : str, title: str, classes: list):
		super().__init__(cat_key, title)
		self.category_labels = classes


	def help(self):
		super().help()
		labels = list()
		for ndx in range(0, len(self.category_labels)):
			labels.append(str(ndx) + '=' + self.category_labels[ndx])
		print(', '.join(labels))


	def get_category(self, result: dict) -> str: 
		"""Get category"""
		cat_id = -1
		try:
			cat_id	= result[self.category_key]
			return self.category_labels[cat_id]
		except KeyError as err:
			bu.error('Category: ' + str(self.category_key) + ' ID: ' + str(cat_id), exception=err)
		except Exception as err:
			bu.error('Category: ' + str(self.category_key) + ' ID: ' + str(cat_id), exception=err)
		return None


	def get_filter_categories(self, filters: list) -> list:
		try:
			# bu.debug(str(filters), force=True)
			if type(filters) != list:
				filters = [ filters ]
			bu.debug(str(self.category_labels))
			res = list()
			for ndx in filters:
				if ndx in range(0,len(self.category_labels)):
					res.append(self.category_labels[ndx])
			return res
			#return [ self.category_labels[int(x)] for x in filters if x in range(0, len(self.get_categories())) ]
		except Exception as err:
			bu.error('Category: ' + str(self.category_key), exception=err)
		return None


	def get_categories(self) -> list:
		try:
			cats = list()
			for cat in self.category_labels:
				if cat in self.categories.keys():
					cats.append(cat)
			return cats 
		except KeyError as err:
			bu.error('Key not found', exception = err)  
		except Exception as err:
			bu.error(exception = err) 
		return None


class BattleStringCategorization(BattleCategorization):
	"""Class for categorization by string"""

	def __init__(self,  cat_key : str, title: str, params: list):
		super().__init__(cat_key, title)
		if params != None and len(params) > 0:
			width = params[0]
			left_align = True  # align long category names left by default
			if len(params) > 1:
				left_align = params[1]
			
			self.set_cat_header_format(width, left_align)


	def get_category(self, result: dict) -> str: 
		"""Get category"""
		try:
			return result[self.category_key]			
		except KeyError as err:
			bu.error('Category key: ' + self.category_key + ' not found', exception=err)
		except Exception as err:
			bu.error('Category: ' +  self.category_key, exception=err)
		return None
	

	def get_categories(self):
		"""Get category keys sorted"""
		try:
			p_number_sort = re.compile(r'^(\d+): .+')
			cats = list(self.categories.keys())
			if (p_number_sort.match(cats[0]) != None):
				ndxs = sorted(range(len(cats)), key=lambda k: int(cats[k][:cats[k].find(':')])  )
				return [ cats[i] for i in ndxs ] 
			else:
				return sorted(cats , key=str.casefold)

		except Exception as err:
			bu.error('Category: ' +  self.category_key, exception=err)
		return None

	def get_filter_categories(self, filters: list) -> list:
		try:
			if type(filters) != list:
				filters = [ filters ]
			return [ x for x in filters if x in self.get_categories() ]
			# return filters
		except Exception as err:
			bu.error('Category: ' + str(self.category_key), exception=err)
		return None

	
	def help(self):
		super().help()
		print(self.title + ' name')


class BattleNumberCategorization(BattleCategorization):
	"""Class for categorization by number"""

	def get_category(self, result: dict) -> str: 
		"""Get category as """
		try:
			return str(result[self.category_key])
		except KeyError as err:
			bu.error('Category: ' + str(self.category_key), exception=err)
			
		except Exception as err:
			bu.error('Category: ' + str(self.category_key), exception=err)
		return None


	def get_categories(self) -> list:
		try:
			return map(str, sorted( map(int, self.categories.keys())))
		except KeyError as err:
			bu.error('Key not found', exception=err)  
		return None


	def get_filter_categories(self, filters: list) -> list:
		try:
			if type(filters) != list:
				filters = [ filters ]
			return list(map(str,filters))
		except Exception as err:
			bu.error('Category: ' + str(self.category_key), exception=err)
		return None


	def help(self):
		super().help()
		print('INTEGER')


class BattleBucketCategorization(BattleCategorization):
	"""Class for categorization by buckets based on value"""

	def __init__(self, cat_key : str, title: str, breaks: list, bucket_format = ''):
		try:
			super().__init__(cat_key, title)
			self.breaks = breaks
			self.bucket_format = bucket_format
			self.category_labels = self.mk_category_labels(breaks, bucket_format)			
		except Exception as err:
			bu.error('BattleBucketCategorization()', exception=err) 


	def help(self):
		super().help()
		labels = list()
		for ndx in range(0, len(self.category_labels)):
			labels.append(str(ndx) + '=' + self.category_labels[ndx])
		print(', '.join(labels))


	def get_category(self, result: dict) -> str: 
		"""Get category"""
		cat_id = -1
		value = None
		try:
			value = result[self.category_key]
			cat_id = self.find_bucket(value, self.breaks)
			return self.category_labels[cat_id]
		except KeyError as err:
			bu.error('Category: ' + str(self.category_key) + ' value: ' + str(value) + ' cat ID: ' + str(cat_id), exception=err)
		except Exception as err:
			bu.error('Category: ' + str(self.category_key) + ' value: ' + str(value) + ' cat ID: ' + str(cat_id), exception=err)
		return None


	def get_categories(self) -> list:
		try:
			cats = list()
			for cat in self.category_labels:
				if cat in self.categories.keys():
					cats.append(cat)
			return cats 
		except KeyError as err:
			bu.error('Key not found', exception = err)  
		except Exception as err:
			bu.error(exception = err) 
		return None


	def get_filter_categories(self, filters: list) -> list:
		try:
			if type(filters) != list:
				filters = [ filters ]
			res = list()
			for ndx in filters:
				if ndx in range(0,len(self.category_labels)):
					res.append(self.category_labels[ndx])
			return res			
		except Exception as err:
			bu.error('Category: ' + str(self.category_key), exception=err)
		return None


	def find_bucket(self, value, bucket_breaks: list):
		"""Find bucket for value"""
		try:
			max = len(bucket_breaks)
			if max <= 1:
				return 0
			ndx = int(max//2)
			if value > bucket_breaks[ndx]:
				return ndx + self.find_bucket(value, bucket_breaks[ndx:])
			elif value < bucket_breaks[ndx]:
				return self.find_bucket(value, bucket_breaks[:ndx])
			else:
				return ndx
		except KeyError as err:
			bu.error('Key not found ndx=' + str(ndx) + ', value=' + str(value), exception = err)  
		except Exception as err:
			bu.error('ndx=' + str(ndx) + ', value=' + str(value), exception = err) 
		return -2


	def mk_category_labels(self, bucket_breaks: list, bucket_format: str) -> list:
		try:
			# add open-ended last category. list.append() modifies by reference self.breaks (not OK)!!
			bucket_breaks = bucket_breaks + [ None ]  
			multiplier  = 1
			prefix = ''
			if bucket_format == '%':
				frmt = '{:2.0f}'
				multiplier = 100
				prefix = '%'
			elif bucket_format == 'float':
				frmt = '{:.2f}'
			elif bucket_format == 'int':
				frmt = '{:.0f}'
			else:
				frmt = '{:.0f}'
			labels = list()
			for ndx in range(0,len(bucket_breaks) - 1):
				low 	= bucket_breaks[ndx]
				high 	= bucket_breaks[ndx +1]

				# None makes open ended category label: '1500 - ' 				
				if low == None: 
					label = '   - '
				else:
					label = frmt.format(low*multiplier) + ' - '
				
				if high == None:
					label = label + '  '
				else:
					label = label + frmt.format(high*multiplier)

				label = label + prefix
				labels.append(label)
			return labels

		except KeyError as err:
			bu.error('KeyError: ndx=' + str(ndx) + ' buckets=' + str(bucket_breaks), exception=err)
		except Exception as err:			
			bu.error('Unknown error', exception=err) 
		return None


class BattleCategory():
	
	## Syntax: Check how the replay JSON files look. The algorithm is counting/recording fields
	_result_fields = {
		'battles'			: [ 'Battles', 'Number of battles', 								7, '{:7.0f}' ],
		'battles%'			: [ '% Battles', 'Share of Battles',								9, '{:9.0%}' ],
		'win'				: [ 'WR', 'Win rate', 												6, '{:6.1%}' ],
		'damage_made'		: [ 'DPB', 'Average Damage', 										5, '{:5.0f}' ],
		'damage_received'	: [ 'DPBr', 'Average Damage  Received', 							5, '{:5.0f}' ],
		'enemies_destroyed'	: [ 'KPB', 'Kills / Battle', 										4, '{:4.2f}' ],
		'DR'				: [ 'DR', 'Damage Ratio', 											5, '{:5.1f}' ],
		'KDR'				: [ 'KDR', 'Kills / Death', 										4, '{:4.1f}' ],
		'enemies_spotted'	: [ 'Spot', 'Enemies spotted per battle', 							4, '{:4.1f}' ],
		'hit_rate'			: [ 'Hit rate', 'Shots hit / all shots made', 						8, '{:8.1%}' ],
		'shots_made'		: [ 'Shots', 'Shots made',			 								5, '{:5.1f}' ],
		'hits_received'		: [ 'Hits R.', 'Hits reveived',		 								7, '{:7.1f}' ],
		'hits_bounced'		: [ 'Bounced R.', 'Hits & bounced reveived',						10, '{:10.1f}' ],
		'in_platoon'		: [ 'Plat%', 'Platoon rate',										6, '{:6.1%}' ],
		'pen_rate'			: [ 'Pen rate', 'Shots pen / shots hit', 							8, '{:8.1%}' ],
		'pen_rate_received'	: [ 'Penned%', 'Received hits pen / hits received',					7, '{:7.1%}' ],
		'splash_rate'		: [ 'Splash%', 'HE splash shots / shots hit',						7, '{:7.1%}' ],
		'splash_rate_received'	: [ 'Splashed%', 'HE splash shots received / hits hit',			9, '{:9.1%}' ],		
		'survived'			: [ 'Surv%', 'Survival rate', 										6, '{:6.1%}' ],
		'time_alive'		: [ 'T alive', 'Time being alive in a battle in secs', 				7, '{:7.0f}' ],
		'alive'				: [ 'Share live', 'Percentage of time being alive in a battle', 	8, '{:8.0%}' ],
		'battle_duration'	: [ 'Duration', 'Battle duration', 									8, '{:8.0f}' ], 
		'distance_travelled': [ 'Distance', 'Distance travelled',								8, '{:8.0f}' ], 
		'top_tier'			: [ 'Top tier', 'Share of games as top tier', 						8, '{:8.0%}' ],
		'player_wins'		: [ 'Player WR', 'Average WR of the player', 						9, '{:9.2%}' ],
		'allies_wins'		: [ 'Allies WR', 'Average WR of allies at the tier of their tank', 	9, '{:9.2%}' ],
		'enemies_wins'		: [ 'Enemies WR', 'Average WR of enemies at the tier of their tank', 10, '{:10.2%}' ],
		'player_battles'	: [ 'Player Btls', 'Average number battles of the player', 			11, '{:11.0f}' ],
		'allies_battles'	: [ 'Allies Btls', 'Average number battles of the allies', 			11, '{:11.0f}' ],
		'enemies_battles'	: [ 'Enemies Btls', 'Average number battles of the enemies', 		12, '{:12.0f}' ],
		'player_damage_dealt'	: [ 'Player Avg Dmg', 'Player Average damage', 					14, '{:14.0f}' ],
		'allies_damage_dealt'	: [ 'Allies Avg Dmg', 'Average damage of the allies', 			14, '{:14.0f}' ],
		'enemies_damage_dealt'	: [ 'Enemies Avg Dmg', 'Average damage of the enemies', 		15, '{:14.0f}' ],
		MISSING_STATS		: [ 'No stats', 'Players without stats avail', 						8, '{:8.1%}']		
	}


	# fields to display in results
	_result_fields_modes = {
		'default'		: [ 'battles',	'win','damage_made', 'enemies_destroyed', 'enemies_spotted', 
							'top_tier', 'DR', 'survived', 'allies_wins', 'enemies_wins'],
		'team'			: [ 'battles',	'win', 'in_platoon', 'player_wins', 'allies_wins','enemies_wins'],
		'team_extended'	: [ 'battles',	'battles%',	'win', 'in_platoon', 'player_wins', 'allies_wins', 'enemies_wins', 
							'player_damage_dealt', 'allies_damage_dealt', 'enemies_damage_dealt', 
							'player_battles','allies_battles','enemies_battles' ],
		'extended'		: [ 'battles',	'battles%',	'win', 'in_platoon', 'damage_made', 'enemies_destroyed', 'enemies_spotted', 
							'top_tier', 'DR', 'KDR', 'hit_rate', 'pen_rate', 'survived', 'alive', 
							'player_wins', 'allies_wins', 'enemies_wins', MISSING_STATS ],
		'dmg_received'	: [ 'battles',	'win','damage_made', 'damage_received', 'enemies_destroyed', 'top_tier', 'DR', 
							'hits_received', 'hits_bounced', 'pen_rate_received', 'splash_rate_received'], 
		'all'			: [ cat for cat in _result_fields.keys() ]
	}

	_team_fields = [ 'wins', 'battles', 'damage_dealt' ]
	
	count_fields = [
		'battles', 
		MISSING_STATS, 
		'mastery_badge'
	]

	_result_ratios = {
		'KDR'					: [ 'enemies_destroyed', 'destroyed' ],
		'DR'					: [ 'damage_made', 'damage_received' ],
		'hit_rate'				: [ 'shots_hit', 'shots_made' ],
		'splash_rate'			: [ 'shots_splash', 'shots_hit' ],
		'splash_rate_received'	: [ 'hits_splash', 'hits_received' ],
		'pen_rate'				: [ 'shots_pen', 'shots_hit' ],
		'pen_rate_received'		: [ 'hits_pen', 'hits_received' ],		
		'dmg_block_rate' 		: [ 'damage_blocked', 'damage_received' ]
	}

	# _extended_stats = False

	result_fields 	= list()
	avg_fields 		= set()
	ratio_fields 	= set()
		

	@classmethod
	def get_modes(cls) -> list:
		return list(cls._result_fields_modes.keys())

	@classmethod
	def get_mode_fields(cls, mode: str):
		try:
			return cls._result_fields_modes[mode]
		except Exception as err:
			bu.error(exception=err)
		return None


	@classmethod
	def set_fields(cls, mode: str = None):
		try:
			if mode not in cls.get_modes():
				bu.error('Mode ' + mode + ' not in defined modes: ' + ', '.join(cls.get_modes()))
				sys.exit(2)

			cls.mode = mode
			cls.result_fields = cls.get_mode_fields(cls.mode)
			cls.result_fields_ratio = set(cls._result_ratios.keys()) & set(cls.result_fields)
			cls.avg_fields = set(cls.result_fields) - set(cls._result_ratios.keys()) - set(cls.count_fields)
		
			for ratio in cls.result_fields_ratio:
				cls.ratio_fields.add(cls._result_ratios[ratio][0])
				cls.ratio_fields.add(cls._result_ratios[ratio][1])
		except Exception as err:
			bu.error('BattleCategory:' ,exception=err)
		

	@classmethod
	def get_result_fields(cls) -> list:
		return cls.result_fields


	@classmethod
	def get_result_fields_all(cls) -> list:
		return cls._result_fields.keys()

	@classmethod
	def get_result_fields_ratio(cls, all: bool = False) -> list:
		if all:
			return cls._result_ratios.keys()
		else:
			return cls.result_fields_ratio


	@classmethod
	def get_avg_fields(cls) -> set:
		return cls.avg_fields


	@classmethod
	def get_ratio_fields(cls) -> set:
		return cls.ratio_fields


	@classmethod
	def get_field_name(cls, field: str) -> str:
		try:
			return cls._result_fields[field][0]
		except Exception as err:
			bu.error(exception=err)

	
	@classmethod
	def get_field_description(cls, field: str) -> str:
		try:
			return cls._result_fields[field][1]
		except Exception as err:
			bu.error(exception=err)

	
	@classmethod
	def get_field_width(cls, field: str):
		try:
			return cls._result_fields[field][2]
		except Exception as err:
			bu.error(exception=err)


	@classmethod
	def get_fields_team(cls) -> list:
		return cls._team_fields

	
	@classmethod
	def calc_ratio(cls, ratio: str, result: dict): 
		try:
			value 	= result[cls._result_ratios[ratio][0]]
			divider = result[cls._result_ratios[ratio][1]]
			if divider != 0:
				if value == None:
					value = 0
				return value / divider
			else:
				if value == 0:
					return 0
				else:
					return float('Inf')
		except Exception as err:
			bu.error('Replay _id=' + result['_id'] , exception=err)
		return None


	@classmethod
	def print_headers(cls, header: str):
		try:
			headers = [ header ]
			for field in cls.result_fields:
				print_format = '{:^' + str(cls.get_field_width(field)) + 's}'
				headers.append(print_format.format(cls.get_field_name(field)))
			print('   '.join(headers))
		except KeyError as err:
			bu.error('Key not found', exception=err)  
		except Exception as err:
			bu.error(exception=err) 
		return None


	@classmethod
	def get_headers(cls, header: str = None) -> list:
		try:
			headers = [ header ]
			for field in cls.result_fields:
				headers.append(cls.get_field_name(field))
			return headers
		except KeyError as err:
			bu.error('Key not found', exception=err)  
		except Exception as err:
			bu.error(exception=err) 
		return None


	@classmethod
	def help(cls):
		try:
			for field in cls.get_result_fields_all():
				print('{:24s} : {}'.format(field, cls.get_field_description(field)))
		except Exception as err:
			bu.error(exception=err) 
		return None



	def __init__(self):
		try:
			
			self.battles 		= 0
			self.missing_stats 	= 0
			self.n_players 		= 0
			self.ratios_ready 	= False
			self.results_ready 	= False
			self.results = collections.defaultdict(def_value_zero)

		except KeyError as err:
			bu.error('BattleCategory(): Key not found', err) 


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
			bu.error('BattleCategory:' ,exception=err)
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


	def get_results(self) -> dict:
		"""Return results as a dict"""
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

	
	def get_results_list(self) -> list:
		"""Return results as a list"""
		if not self.results_ready:
			bu.error('Stats have not been calculated yet. call calc_results() before get_results()')
		try:
			results = list()
			for field in self.result_fields:
				results.append(self._result_fields[field][3].format(self.results[field]))
			return results
		except KeyError as err:
			bu.error('Key not found', err)  
		except Exception as err:
			bu.error(exception=err) 
		return None
	

	def print_results(self):
		if not self.results_ready:
			bu.error('Stats have not been calculated yet. call calc_results() before printing()')
		try:
			results = []
			for field in self.result_fields:
				results.append(self._result_fields[field][3].format(self.results[field]) )				
			print(' : '.join(results))
			return True
		except KeyError as err:
			bu.error('Key not found', err)  
		except Exception as err:
			bu.error(exception=err) 
		return False


class PlayerHistogram():
	def __init__(self, field: str, name: str, fields : list, factor: float, cat_format: str ):
		self.field 		= field
		if self.field == 'wins':
			self.name 		= StatFunc.get_title()
		else:
			self.name = name
		self.fields 	= fields
		self.cat_factor = factor
		self.cat_format = cat_format
		self.ncat 		= len(self.fields) - 1
		self.allies 	= [0] * self.ncat
		self.enemies 	= [0] * self.ncat
		self.total 		= [0] * self.ncat
		self.results 	= None
	
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


	def calc_results(self):
		N_enemies = 0
		N_allies = 0
		res = dict()
		# calculate buckets 
		for cat in range(0, self.ncat):
			N_enemies += self.enemies[cat]
			N_allies += self.allies[cat]
		N_total = N_allies + N_enemies
	
		for cat in range(0, self.ncat):			
			stat = dict()
			stat['allies'] 		= self.allies[cat]
			stat['allies%'] 	= stat['allies'] / N_allies
			stat['enemies'] 	= self.enemies[cat]
			stat['enemies%'] 	= stat['enemies'] / N_enemies
			stat['total'] 		= self.allies[cat] + self.enemies[cat]
			stat['total%'] 		= stat['total'] / N_total
			
			res[self.get_category_name(cat)] = stat

		self.results = res
		return res


	def get_results(self) -> dict:
		return self.results

	
	def get_results_list(self) -> list:
		try:
			if (self.results != None):
				res = list()
				res.append([self.name, "Allies", "Allies %",  "Enemies", "Enemies %" ,"TOTAL", "TOTAL %"])
				for cat in self.results:
					stat = self.results[cat]
					res.append([ cat, stat['allies'], stat['allies%'],  
							stat['enemies'], stat['enemies%'], 
							stat['total'], stat['total%'] ])
				return res
			else:
				bu.error('Results have not been calculated yet.')
				return None
		except Exception as err:
			bu.error(exception=err)
		return None


	def print(self):
		try:
			if (self.results != None):
				print("\n{:12s} | {:13s} | {:13s} | {:13s}".format(self.name, "Allies", "Enemies", "TOTAL"))
				for cat in self.results:
					stat = self.results[cat]
					print("{:12s} | {:5d} ({:5.1%}) | {:5d} ({:5.1%}) | {:5d} ({:5.1%})".format(cat, stat['allies'], stat['allies%'], stat['enemies'], stat['enemies%'], stat['total'], stat['total%'] ))
			else:
				bu.error('Results have not been calculated yet.')
		except Exception as err:
			bu.error(exception=err)
		return None


class ErrorCatchingArgumentParser(argparse.ArgumentParser):
	def exit(self, status=0, message=None):
		if status:
			if message != None:
				raise UserWarning(message)
			else:
				raise UserWarning()


## main() -------------------------------------------------------------

async def main(argv):
	global wg, wi, WG_APP_ID, OPT_MODE_DEFAULT, OPT_EXPORT_CSV_FILE, OPT_EXPORT_JSON_FILE 
	# set the directory for the script
	current_dir = os.getcwd()
	os.chdir(os.path.dirname(sys.argv[0]))

	## Default options:
	OPT_MODE_DEFAULT 	= 'default'
	OPT_MODE_HELP		= 'help'
	OPT_MODES 			= BattleCategory.get_modes() + [OPT_MODE_HELP]
	OPT_DB				= False
	OPT_HIST			= False
	OPT_STAT_FUNC		= StatFunc.get_default()
	OPT_WORKERS_N 		= 10
	# OPT_EXPORT_CSV_FILE	 = 'export.csv'
	# OPT_EXPORT_JSON_FILE = 'export.json'

	#WG_ACCOUNT 		= None 	# format: nick@server, where server is either 'eu', 'ru', 'na', 'asia' or 'china'. 
	  					 	# China is not supported since WG API stats are not available there
	#WG_ID			= None  # WG account_id in integer format. 
							# WG_ACCOUNT will be used to retrieve the account_id, but it can be set directly too
	WG_RATE_LIMIT	= 10  ## WG standard. Do not edit unless you have your
						  ## own server app ID, it will REDUCE the performance
	
	## VERY unlikely you have a DB set up
	DB_SERVER 	= 'localhost'
	DB_PORT 	= 27017
	DB_TLS		= False
	DB_CERT_REQ = False
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
					configAnalyzer		= config['ANALYZER']
					OPT_MODE_DEFAULT	= configAnalyzer.getboolean('mode', OPT_MODE_DEFAULT)
					OPT_HIST			= configAnalyzer.getboolean('histograms', OPT_HIST)
					OPT_STAT_FUNC		= configAnalyzer.get('stat_func', fallback=OPT_STAT_FUNC)
					OPT_WORKERS_N 		= configAnalyzer.getint('workers', OPT_WORKERS_N)
					OPT_EXPORT_CSV_FILE = configAnalyzer.get('csv_file', OPT_EXPORT_CSV_FILE)
					OPT_EXPORT_JSON_FILE= configAnalyzer.get('json_file', OPT_EXPORT_JSON_FILE)
					res_categorizations = configAnalyzer.get('categorizations', None)
					if res_categorizations != None:
						res_categorizations.replace(' ','')
						BattleCategorizationList.set_categorizations_default(res_categorizations.split(','))
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
					#WG_ID			= configWG.getint('wg_id', WG_ID)
					#WG_ACCOUNT		= configWG.get('wg_account', WG_ACCOUNT)
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
					DB_TLS		= configDB.getboolean('db_tls', DB_TLS)
					DB_CERT_REQ = configDB.getboolean('db_tls_req', DB_CERT_REQ)
					DB_AUTH 	= configDB.get('db_auth', DB_AUTH)
					DB_NAME 	= configDB.get('db_name', DB_NAME)
					DB_USER		= configDB.get('db_user', DB_USER)
					DB_PASSWD 	= configDB.get('db_password', DB_PASSWD)
					DB_CERT 	= configDB.get('db_tls_cert_file', DB_CERT)
					DB_CA 		= configDB.get('db_tls_ca_file', DB_CA)
			except (KeyError, configparser.NoSectionError)  as err:
				bu.error(exception=err)

		parser = ErrorCatchingArgumentParser(description='Analyze Blitz replay JSON files from WoTinspector.com. Use \'upload_wotb_replays.py\' to upload the replay files first.')

		parser.add_argument('--mode', default=OPT_MODE_DEFAULT, choices=OPT_MODES, help='Select stats to print out (columns). Options: ' + ', '.join(OPT_MODES))
		parser.add_argument('--extra', choices=BattleCategorizationList.get_categorizations_all(), default=None, nargs='*', help='Print extra categories: ' + ', '.join( cat + '=' + BattleCategorizationList.get_categorization_title(cat) for cat in BattleCategorizationList.get_categorizations_all()))
		parser.add_argument('--only_extra', action='store_true', default=False, help='Print only the extra categories')
		parser.add_argument('--hist', action='store_true', default=OPT_HIST, help='Print player histograms: ' + ', '.join( histogram_fields[k][0] for k in histogram_fields))
		# parser.add_argument('--output', default='plain', choices=['plain', 'db'], help='Select output mode: plain text or database')
		parser.add_argument('-id', dest='account_id', type=int, default=None, help='WG account_id to analyze. Replays without the account_id will be ignored.')
		parser.add_argument('-a', '--account', type=str, default=None, help='WG account name to analyze. Format: ACCOUNT_NAME@SERVER')
		parser.add_argument('--stat_func', default=OPT_STAT_FUNC, choices=StatFunc.get_stat_funcs(), help='Select how to calculate for ally/enemy performance: tank-tier stats, global player stats')
		parser.add_argument('-u', '--url', action='store_true', default=False, help='Print replay URLs')
		parser.add_argument('--tankfile', type=str, default='tanks.json', help='JSON file to read Tankopedia from. Default is "tanks.json"')
		parser.add_argument('--mapfile', type=str, default='maps.json', help='JSON file to read Blitz map names from. Default is "maps.json"')
		parser.add_argument('--json', action='store_true', default=False, help='Export data in JSON format')
		parser.add_argument('--csv', action='store_true', default=False, help='Export data in CSV format')
		parser.add_argument('--replays', action='store_true', default=False, help='Export replay info in JSON format')
		parser.add_argument('-o','--outfile', type=str, default='-', metavar="OUTPUT", help='File to write results. Default STDOUT')
		parser.add_argument('--db', action='store_true', default=OPT_DB, help='Use DB - You are unlikely to have it')
		parser.add_argument('--filters', type=str, default=None, help='Filter replays based on categories. Filters given in JSON format.\nUse array "[]" for multiple filters/values. see --mode help.\nExample: : [ {"tank_tier" : [8,9,10] }, { "player_wins" : 5 }]')
		parser.add_argument('--filters_db', type=str, default=None, help='[only for DB setup] Filter replays in DB based on categories. Filters given in MongoDB JSON format. See --mode help')
		parser.add_argument('--min', type=int, default=None, help='Only select replays from players with minimum number of replays in the dataset')
		parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
		parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
		parser.add_argument('-s', '--silent', action='store_true', default=False, help='Silent mode')
		parser.add_argument('-l', '--log', action='store_true', default=False, help='Enable file logging')
		parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='*', help='Files/dirs to read. Use \'-\' for STDIN, "db:" for database')
		## parser.add_argument('--help_filters', action='store_true', default=False, help='Extended help for --filters')		

		try:
			args = parser.parse_args()			
		except Exception as err:
			bu.error('Invalid arguments', exception=err)			
			sys.exit(0)

		# res_categories = BattleCategorization.get_categorizations(OPT_CATEGORIZATIONS, args)

		bu.set_log_level(args.silent, args.verbose, args.debug)
		# bu.set_progress_step(250)  						# Set the frequency of the progress dots. 

		#### Connect to MongoDB. You are unlikely to have this set up... 
		bu.debug('DB_SERVER: ' + DB_SERVER)
		bu.debug('DB_PORT: ' + str(DB_PORT))
		bu.debug('DB_TLS: ' + "True" if DB_TLS else "False")
		bu.debug('DB_AUTH: ' + DB_AUTH)
		bu.debug('DB_NAME: ' + DB_NAME)
		
		client = None
		db = None
		if args.db:
			try:
				client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, tls=DB_TLS, tlsAllowInvalidCertificates=DB_CERT_REQ, tlsCertificateKeyFile=DB_CERT, tlsCAFile=DB_CA)
				db = client[DB_NAME]
				bu.debug('Database connection initiated')
			except Exception as err: 
				bu.error("Could no initiate DB connection: Disabling DB", err) 
				args.db = False				
		else:
			bu.debug('No DB in use')

		# TBD
		if args.mode == OPT_MODE_HELP: 
			await help_extended(db, parser)
			sys.exit(0)
		elif len(args.files) == 0:
			raise UserWarning('No FILES argument given: No replays to analyse')

		if args.log:
			await bu.set_file_logging('analyze_wotb_replays', add_timestamp=True)

		StatFunc.set_stat_func(args.stat_func)
		# rebase file arguments due to moving the working directory to the script location
		args.files = bu.rebase_file_args(current_dir, args.files)

		wg = WG(WG_APP_ID, args.tankfile, args.mapfile, stats_cache=True, rate_limit=WG_RATE_LIMIT)
		wi = WoTinspector(rate_limit=10)

		if args.account != None:
			args.account_id = await wg.get_account_id(args.account)
			bu.debug('WG  account_id: ' + str(args.account_id))

		# try:
		BattleCategory.set_fields(args.mode)
		replayQ  = asyncio.Queue(maxsize=1000)			
		reader_tasks = []
		# Make replay Queue

		scanner_task = asyncio.create_task(mk_replayQ(replayQ, args, db))
		bu.set_counter('Reading replays', 10)
		# Start tasks to process the Queue
		for i in range(OPT_WORKERS_N):
			reader_tasks.append(asyncio.create_task(replay_reader(replayQ, i, args)))
			bu.debug('ReplayReader ' + str(i) + ' started')

		bu.debug('Waiting for the replay scanner to finish')
		await asyncio.wait([scanner_task])
		bu.finish_progress_bar()
		# bu.debug('Scanner finished. Waiting for replay readers to finish the queue')
		await replayQ.join()
		await asyncio.sleep(0.1)
		bu.debug('Replays read. Cancelling Readers and analyzing results')
		for task in reader_tasks:
			task.cancel()
			await asyncio.sleep(0.1)	
		results = []
		players = set()
		replays	= dict()
		for res in await asyncio.gather(*reader_tasks):
			results.extend(res[0])
			players.update(res[1])
			replays.update(res[2])
		
		if len(players) == 0:
			raise Exception("No players found to fetch stats for. No replays found?")

		if args.min != None:
			results = filter_min_replays_by_player(results, args.min)
			
		# Filter based on non-stats filters
		results = filter_results(results, args.filters, stats_filters=False)	
		players = get_players(results)
		
		(player_stats, stat_id_map) = await process_player_stats(players, OPT_WORKERS_N, args, db)
	
		# Filter based on stats filters
		results = filter_results(results, args.filters, stats_filters=True)
		bu.debug(str(len(replays)) + ' replays to process')
		replays = filter_replays(replays, results)				
		
		bu.debug('Number of player stats: ' + str(len(player_stats)))
		teamresults = calc_team_stats(results, player_stats, stat_id_map, args)

		blt_cat_list = process_battle_results(teamresults, args)
		bu.verbose_std('WR = ' + StatFunc.get_title())
		blt_cat_list.print_results()

		histograms = None
		if args.hist:
			histograms = process_player_dist(results, player_stats, stat_id_map)
			print_player_dist(histograms)			
		
		if args.json:
			await export_json(args, blt_cat_list, histograms)
		
		if args.csv:
			await export_csv(args, blt_cat_list, histograms)	
		
		if args.replays:
			await export_replays(replays)

		bu.debug('Finished. Cleaning up..................')
		# except Exception as err:
		# 	bu.error(exception=err)
	except UserWarning as err:
		bu.warning(str(err))
	except asyncio.CancelledError:
		pass
	except Exception as err:
		bu.error(exception=err)
	finally:
		## Need to close the aiohttp.session since Python destructors do not support async methods... 
		if wg != None: 
			await wg.close()
		if wi != None: 
			await wi.close()		
	return None


async def export_csv(args : argparse.Namespace, blt_cat_list: BattleCategorizationList, histograms: dict = None):
	"""Export results into a CSV file"""
	try:
		rows = blt_cat_list.get_results_list()
		if args.hist:
			rows.append('')
			rows.append(['Player Histograms'])
			for histogram in histograms.values():
				rows.append('')
				rows.extend(histogram.get_results_list())
		if args.outfile == '-':
			export_file = OPT_EXPORT_CSV_FILE
		else:
			export_file = args.outfile
		with open(export_file, 'w') as csvfile:  
			# creating a csv writer object  
			csvwriter = csv.writer(csvfile)        					
			csvwriter.writerows(rows)
			bu.verbose_std('Results exported to: ' + export_file)		
	except Exception as err:
		bu.error(exception=err)


async def export_json(args : argparse.Namespace, blt_cat_list: BattleCategorizationList, histograms: dict = None):
	"""Export results into a JSON file"""
	try:
		res = blt_cat_list.get_results_json()
		if histograms != None:
			res_hist = dict()
			for histogram in histograms.keys():
				res_hist[histogram] = histograms[histogram].get_results()
			res['histograms'] = res_hist
		if args.outfile == '-':
			export_file = OPT_EXPORT_JSON_FILE
		else:
			export_file = args.outfile
		await bu.save_JSON(export_file, res, pretty=False) ## Excel does not understand pretty JSON
		bu.verbose_std('Results exported to: ' + export_file)
	except Exception as err:
		bu.error(exception=err)


async def help_extended(db : motor.motor_asyncio.AsyncIOMotorDatabase = None, parser: argparse.ArgumentParser = None):
	"""Help for --filters"""
	if parser != None:
		parser.print_help()
	# Result category options

	# Results fields
	print('\n-------------------------------------------------------------------')
	print('| Result fields                                                   |')
	print('-------------------------------------------------------------------\n')
	
	BattleCategory.help()

	print('\n-------------------------------------------------------------------')
	print('| Filters                                                         |')
	print('-------------------------------------------------------------------\n')
	print("\t--filters '{ \"categorization\" : category }'")
	print('\tThese filters let you filter only battles matching the filter. The filter has been proper JSON dict.')
	print("\tExample: --filter '{ \"room_type\" :1 }'")
	print('')
	BattleCategorizationList.help()
	print('\n-------------------------------------------------------------------')
	print('| Rounding errors                                                 |')
	print('-------------------------------------------------------------------\n')
	print('Yes, the numbers are rounded UP because reasons.')
	print('If interested why please read this: ')
	print('\thttps://realpython.com/python-rounding/#pythons-built-in-round-function')
	print('No, I am not planning to fix these for now.')	

	# Filter usage
	print('\n-------------------------------------------------------------------')
	print('| DB Filter usage                                                 |')
	print('-------------------------------------------------------------------\n')
	print('You are unlikely to have DB setup required for these filters. This is "Jylpah\'s special"')
	print('')
	print("Syntax: --filters_db '{ \"replay.param\": VALUE, \"replay.param2\": { \"$MONGO_OPERATOR\" : VALUE }, ... }'")
	print("\tExample: --filters_db '{ \"data.summary.protagonist\": ACCOUNT_ID, \"data.summary.battle_start_timestamp\": { \"$gt\" : 1602853200 } }'")
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


async def export_replays(replays: dict):
	"""Export replays"""
	try:
		replay_dir = OPT_EXPORT_REPLAY_DIR +  '_' + bu.get_date_str()
		bu.verbose_std('Exporting replays to ' + replay_dir)
		if not os.path.isdir(replay_dir):
			os.mkdir(replay_dir)

		i = 0
		for replay in replays.values():
			filename = wg.get_replay_filename(replay)
			async with aiofiles.open(os.path.join(replay_dir, filename), 'w', encoding="utf8") as f:
				await f.write(json.dumps(replay, indent=4))
			i += 1
		bu.verbose_std( str(i) + ' replays exported')
	except Exception as err:
		bu.error(exception=err)


## move the class PlayerHistogram?
def set_histogram_buckets(json: dict):
	global histogram_fields
	try:
		for field in json.keys():
			histogram_fields[field][1] = json[field]
	except (KeyError, Exception) as err:
		bu.error(exception=err)
	return histogram_fields


def get_players(results: list) -> set:
	try:
		players = set()
		for res in results:
			players.add(res['player'])
			players.update(res['allies'])
			players.update(res['enemies'])				
		return players
	except Exception as err:
		bu.error(exception=err)
	return None


def filter_min_replays_by_player(results: list, min_replays: int) -> list:
	try:
		replay_counter = collections.defaultdict(def_value_zero)
		for res in results:
			replay_counter[res['protagonist']] +=1
		
		players_enough_replays = set()
		for player in replay_counter.keys():
			if replay_counter[player] >= min_replays:
				players_enough_replays.add(player)
		
		res_ret = list()
		for res in results:
			if res['protagonist'] in players_enough_replays:
				res_ret.append(res)
		bu.verbose_std('Replays after filtering: ' + str(len(res_ret)))	
		return res_ret

	except Exception as err:
		bu.error(exception=err)
	return None	


def process_player_dist(results: list, player_stats: dict, stat_id_map: dict) -> dict:
	"""Process player distribution"""
	hist_stats = dict()
	try:
		for field in histogram_fields:
			hist_stats[field] = PlayerHistogram(field, histogram_fields[field][0], histogram_fields[field][1], histogram_fields[field][2], histogram_fields[field][3] )

		for result in results:
			for player in result['allies'] | result['enemies']:   # union of Sets
				player_remapped = stat_id_map[player]
				if player_remapped in player_stats:
					if player in result['allies']:
						for stat_field in hist_stats:
							hist_stats[stat_field].record_ally(player_stats[player_remapped][stat_field])
					else:
						for stat_field in hist_stats:
							hist_stats[stat_field].record_enemy(player_stats[player_remapped][stat_field])

		for stat_field in hist_stats:
			hist_stats[stat_field].calc_results()
		
		return hist_stats	
	except Exception as err:
		bu.error(exception=err)

	return None


def print_player_dist(histograms: dict):
	# Print results
	print('\nPlayer Histograms______', end='', flush=True)
	for hist in histograms.values():
		print('')
		hist.print()


def process_battle_results(results: dict, args : argparse.Namespace):
	"""Process replay battle results""" 
	try:
		url 		= args.url
		cats 		= BattleCategorizationList.get_categorizations(args)
		blt_cat_list= None
		
		# remove if? 
		if len(results) > 0:
			blt_cat_list = BattleCategorizationList(cats)
			for result in results:
				blt_cat_list.record_result(result)
				if url:
					blt_cat_list.record_url(result)

			blt_cat_list.calc_results()
		else:
			warning_txt = 'No replays to process '
			if args.filters != None: 
				warning_txt = warning_txt + 'after filtering'
			bu.warning(warning_txt)

	except Exception as err:
		bu.error(exception=err)
	return blt_cat_list


def filter_results(results: list, filters_json : str, stats_filters=None) -> bool:
	"""Filter replays based on battle category filters"""
	try:
		if filters_json == None: 
			return results
		filters = json.loads(filters_json)
		bu.debug(str(filters))
		if type(filters) != dict:
			bu.error('invalid --filter arguments: ' + str(filter))
			sys.exit(1)
		
		filter_cats = BattleCategorizationList.get_categorizations_stats(filters.keys(), stats_filters)
		filter_cats = BattleCategorizationList(filter_cats)
		if filter_cats.len() == 0:
			bu.debug('Nothing to filter')
			return results

		bu.verbose('Filters applied:')
		for category in filter_cats:
			cat = category.category_key
			categorization = filter_cats.get_categorization(cat)
			bu.debug('Category: ' + cat +  ' filters: ' + str(filters))
			filters[cat] = categorization.get_filter_categories(filters[cat])
			bu.verbose('  ' + cat + ' = ' + ','.join(filters[cat]))		
		bu.verbose('')

		res = list()
		for result in results:
			try:
				cats = filter_cats.get_categories(result)
				bu.debug(str(cats))
				ret = False
				for cat in cats:
					if cats[cat] not in filters[cat]:
						ret = False
						break
					ret = True
				if ret:
					res.append(result)
			except Exception as err:
				bu.error(exception=err)
		if len(res) == 0:
			raise UserWarning('No replays left after filtering')
		bu.verbose_std('Replays after filtering: ' + str(len(res)))	
		return res
	except UserWarning:
		raise
	except Exception as err:
		bu.error(exception=err)
	return None


def filter_replays(replays_in: dict, results: list) -> list:
	"""Filter source replays based on (filtered) results"""
	replays = dict()
	for res in results:
		try:
			# bu.debug('res[_id] : ' + res['_id'], force=True)			
			_id = res['_id']
			replays[_id] = replays_in[_id]
		except Exception as err:
			bu.error(exception=err)	
	return replays
	
	

async def process_player_stats(players, N_workers: int, args : argparse.Namespace, db : motor.motor_asyncio.AsyncIOMotorDatabase) -> dict:
	"""Start stat_workers to retrieve and store players' stats"""
	## Create queue of player-tank pairs to find stats for
	try:
		statsQ = asyncio.Queue()
		bu.debug('Create player stats queue: ' + str(len(players)) + ' players')
		stat_id_map = dict()
		stat_ids = set()

		stat_id_map_func = globals()[StatFunc.get_stat_id_func()]

		for player in players:
			stat_id_map[player] = stat_id_map_func(player)
			stat_ids.add(stat_id_map[player])

		bu.set_progress_bar('Fetching player stats', len(stat_ids), 25, True)	
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
		player_stats = dict()
		stat_id_remap = dict()

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
	#stat_types = list()
	stat_types = BattleCategory.get_fields_team()
		
	for result in results:
		try:
			missing_stats = 0
			n_players = len(result['allies']) + len(result['enemies'])
			n_allies = collections.defaultdict(def_value_zero)
			allies_stats = collections.defaultdict(def_value_zero)
			n_enemies = collections.defaultdict(def_value_zero)
			enemies_stats = collections.defaultdict(def_value_zero)
						
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
						result['player_' + stat] = player_stats[player_mapped][stat]

			for stat in stat_types:
				if  n_allies[stat] > 0:
					result['allies_' + stat] = allies_stats[stat] / n_allies[stat]
				else:
					bu.debug('No allies stats for: ' + str(result))
				
				if  n_enemies[stat] > 0:
					result['enemies_' + stat] = enemies_stats[stat] / n_enemies[stat]
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
	stats 			= dict()
	stat_id_remap 	= dict()

	stat_db_func = globals()[StatFunc.get_db_func()]
	stat_wg_func = globals()[StatFunc.get_wg_func()]

	bu.debug("workedID: " + str(workerID) + ' started', id=workerID)
	try:
		while True:
			stat_id = await queue.get()
			try:
				bu.debug('Stat_id: ' + stat_id, id=workerID)
				bu.print_progress()
				stats_tmp = None
	
				# Try cache first
				pruned_stat_id = prune_stat_id(stat_id)
				if (pruned_stat_id not in stats):
					stats_tmp = await stat_wg_func(pruned_stat_id, cache_only = True)
				else:
					stat_id_remap[stat_id] = pruned_stat_id
					continue

				if (stats_tmp != None):
					stats[pruned_stat_id] = stats_tmp
					stat_id_remap[stat_id] = pruned_stat_id					
					continue
				
				# try DB
				stats[stat_id] = await stat_db_func(db, stat_id)				
				bu.debug('get_db_' + args.stat_func + '_stats returned: account_id=' + str(get_account_id_f_stat_id(stat_id)) + ': ' + str(stats[stat_id]), workerID)

				# no DB stats found, trying WG AP
				if (stats[stat_id] == None):							
					stats[pruned_stat_id] = await stat_wg_func(pruned_stat_id)
					stat_id_remap[stat_id] = pruned_stat_id						
					del stats[stat_id]
					bu.debug('get_wg_' + args.stat_func + '_stats returned: account_id=' + str(get_account_id_f_stat_id(pruned_stat_id)) + ': ' + str(stats[pruned_stat_id]), workerID)

			except KeyError as err:
				bu.error('Key not found', err, id=workerID)
			except Exception as err:
				bu.error('Unexpected error', err, id=workerID)
			finally:
				queue.task_done()

	except (asyncio.CancelledError, concurrent.futures.CancelledError):
		bu.debug('Stats queue is empty', id=workerID)		
	except Exception as err:
		bu.error(exception=err, id=workerID)
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
			bu.error('Error in pruning empty stats', err, id=workerID)
	# bu.debug('Returning stats & exiting')		
	return (stats, stat_id_remap)
	

## player stat functions: tank
async def get_wg_tank_stats(stat_id_str: str, cache_only = False) -> dict:
	"""Get player stats from WG. Returns WR per tier of tank_id"""
	try:
		(account_id, tank_id) = str2ints(stat_id_str)
		
		# 'battles' must always be there
		hist_stats = [ 'all.' + x for x in histogram_fields.keys() ]
		hist_stats.append('tank_id')

		player_stats = await wg.get_player_tank_stats(account_id, [ tank_id ], hist_stats, cache_only = cache_only)
		#bu.debug('account_id: ' + str(account_id) + ' ' + str(player_stats))

		return tank_stats_helper(player_stats)

	except KeyError as err:
		bu.error('account_id: ' + str(account_id) + ' tank_id:' + str(tank_id) +' : Key not found', err)
	except Exception as err:
		bu.error(exception=err)
	return None


async def get_db_tank_stats(db : motor.motor_asyncio.AsyncIOMotorDatabase, stat_id_str: str) -> dict:
	"""Get player stats from MongoDB (you are very unlikely to have it, unless you have set it up)"""
	if db == None:
		return None
	try:
		dbc = db[DB_C_TANK_STATS]
		( account_id, tank_id, battletime ) = str2ints(stat_id_str)
		time_buffer = 2*7*24*3600

		hist_stats = [ 'all.' + x for x in histogram_fields.keys() ]
		hist_stats.append('tank_id')
		hist_stats.append('account_id')
		hist_stats.append('last_battle_time')
		project = dict()
		for stat in hist_stats:			
			project[stat] = True
		project['_id'] = False
		bu.debug('find(): tank_id={} account_id={} last_battle_time>={}'.format(tank_id, account_id, battletime - time_buffer))
		cursor = dbc.find({ '$and': [ { 'tank_id' : tank_id }, { 'account_id': account_id }, { 'last_battle_time': { '$gte': battletime - time_buffer }} ] }, projection=project).sort('last_battle_time',-1).limit(1)

		# pipeline = 	[ { '$match': { '$and': [ { 'account_id': account_id }, { 'last_battle_time': { '$lte': battletime + time_buffer }}, { 'tank_id' : tank_id } ]}}, 
		# 		{ '$sort': { 'last_battle_time': -1 }}, 
		# 		{ '$group': { '_id': '$tank_id', 'doc': { '$first': '$$ROOT' }}}, 
		# 		{ '$replaceRoot': { 'newRoot': '$doc' }}, 
		# 		{ '$project': { '_id': 0 }} ]

		# cursor = dbc.aggregate(pipeline, allowDiskUse=True)
		#cursor = dbc.aggregate(pipeline)

		stats = await cursor.to_list(1)
		# store cache
		# await wg.save_tank_stats(account_id, [tank_id], stats)
		
		return tank_stats_helper(stats) 	
				
	except Exception as err:
		bu.error('account_id: ' + str(account_id) + ' Error', err)
	return None


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

		return tank_stats_helper(player_stats)

	except KeyError as err:
		bu.error('account_id: ' + str(account_id) + ' Key not found', err)
	except Exception as err:
		bu.error(exception=err)
	return None


## player stat functions: tier_x
async def get_wg_tier_x_stats(stat_id_str: str, cache_only = False) -> dict:
	"""Get player stats from WG. Returns WR per tier of tank_id"""
	try:
		account_id = int(stat_id_str)
		tier_tanks = wg.get_tanks_by_tier(10)

		# 'battles' must always be there
		hist_stats = [ 'all.' + x for x in histogram_fields.keys() ]
		hist_stats.append('tank_id')

		player_stats = await wg.get_player_tank_stats(account_id, tier_tanks, hist_stats, cache_only = cache_only)
		#bu.debug('account_id: ' + str(account_id) + ' ' + str(player_stats))

		return tank_stats_helper(player_stats)

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

		pipeline = 	[ { '$match': { '$and': [ { 'account_id': account_id }, { 'last_battle_time': { '$lte': battletime + time_buffer }}, { 'tank_id' : {'$in': tier_tanks} } ]}}, 
				{ '$sort': { 'last_battle_time': -1 }}, 
				{ '$group': { '_id': '$tank_id', 'doc': { '$first': '$$ROOT' }}}, 
				{ '$replaceRoot': { 'newRoot': '$doc' }}, 
				{ '$project': { '_id': 0 }} ]

		# cursor = dbc.aggregate(pipeline, allowDiskUse=True)
		cursor = dbc.aggregate(pipeline)

		stats = await cursor.to_list(1000)
		# await wg.save_tank_stats(account_id, tier_tanks, stats)
		return tank_stats_helper(stats) 
		
	except Exception as err:
		bu.error('account_id: ' + str(account_id) + ' Error', err)
	return None


async def get_db_tier_x_stats(db : motor.motor_asyncio.AsyncIOMotorDatabase, stat_id_str: str) -> dict:
	"""Get player stats from MongoDB (you are very unlikely to have it, unless you have set it up)"""
	if db == None:
		return None
	try:		
		( account_id, battletime ) = str2ints(stat_id_str)
		return await get_db_tank_tier_stats(db,get_stat_id(account_id, 10, battletime) )
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
				{ '$group': { '_id': { 'tank_id':'$tank_id'}, 'doc': { '$first': '$$ROOT' }}}, 
				{ '$replaceRoot': { 'newRoot': '$doc' }}, 
				{ '$project': { '_id': 0 }} ]

		# cursor = dbc.aggregate(pipeline, allowDiskUse=True)
		cursor = dbc.aggregate(pipeline)

		stats = await cursor.to_list(1000)
		# await wg.save_tank_stats(account_id, [], stats)
		return tank_stats_helper(stats) 
				
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


def tank_stats_helper(stat_list: list):
	"""Helpher func got get_db_tank_tier_stats() and get_wg_tank_tier_stats()"""
	try:
		if stat_list == None: 
			return None
		stats = collections.defaultdict(def_value_zero)
		hist_fields = histogram_fields.keys()
		if 'battles' not in hist_fields:
			bu.error('\'battles\' must be defined in \'histogram_fields\'')
			return None

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
		stats = dict()
		hist_fields = histogram_fields.keys()
		if 'battles' not in hist_fields:
			bu.error('\'battles\' must be defined in \'histogram_fields\'')
			return None
		
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
			if args.filters_db  != None:
				bu.debug(str(args.filters_db))
				filters = json.loads(args.filters_db)
				bu.debug(json.dumps(filters, indent=2))
				cursor = dbc.find(filters)
			else:
				# select all
				cursor = dbc.find(dict())
			bu.debug('Reading replays...')	
			async for replay_json in cursor:
				_id = replay_json['_id']
				#del(replay_json['_id'])
				await queue.put(await mk_readerQ_item(replay_json, _id=_id))
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
					await queue.put(await mk_readerQ_item(replay_json, filename=line))
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
					await queue.put(await mk_readerQ_item(replay_json, filename=fn))
					bu.debug('File added to queue: ' + fn)
					Nreplays += 1
				elif os.path.isdir(fn):
					with os.scandir(fn) as dirEntry:
						for entry in dirEntry:
							if entry.is_file() and (p_replayfile.match(entry.name) != None): 
								bu.debug(entry.name)
								replay_json = await bu.open_JSON(entry.path, wi.chk_JSON_replay)
								await queue.put(await mk_readerQ_item(replay_json, filename=entry.name))
								bu.debug('File added to queue: ' + entry.path)
								Nreplays += 1
			except Exception as err:
				bu.error(exception=err)					
	return Nreplays


async def mk_readerQ_item(replay_json, filename : str = None, _id: str = None) -> list:
	"""Make an item to replay queue"""
	global REPLAY_N
	REPLAY_N +=1
	try:
		if wi.chk_JSON_replay(replay_json): 
			if not '_id' in replay_json:
				if _id  == None:			
					_id = wi.read_replay_id(replay_json)
				replay_json['_id'] = _id
		else:
			replay_json = None # mark error
	except Exception as err:
		bu.error(exception=err)
	
	if filename != None:
		return [replay_json, REPLAY_N, os.path.basename(filename) ]
	elif _id != None: 
		return [replay_json, REPLAY_N, 'DB: ' + _id ]
	else:
		return None


async def replay_reader(queue: asyncio.Queue, readerID: int, args : argparse.Namespace):
	"""Async Worker to process the replay queue"""
	#global SKIPPED_N
	results = []
	playerstanks = set()
	replays		 = dict()
	try:
		while True:
			item = await queue.get()
			replay_json 	= item[0]
			replayID 		= item[1]
			replay_file 	= item[2]

			try:
				# msg_str = 'Replay[' + str(replayID) + ']: ' 
				if replay_json == None:
					bu.warning('Invalid replay. Skipping: '  + (replay_file if replay_file != None else '') )
					queue.task_done()
					continue
						
				## Read the replay JSON
				bu.debug('reading replay', readerID)
				result = await read_replay_JSON(replay_json, args)
				bu.print_progress()
				if result == None:
					bu.warning('Invalid replay ' + (replay_file if replay_file != None else '') )
					queue.task_done()
					continue
				
				playerstanks.update(set(result['allies']))
				playerstanks.update(set(result['enemies']))	
				playerstanks.update(set([result['player']]))
				_id = wi.read_replay_id(replay_json)
				replays[_id] = replay_json
				results.append(result)

				bu.debug('Marking replay [' + str(replayID) + '] done', id=readerID)
						
			except Exception as err:
				bu.error(exception=err)
			queue.task_done()
	except (asyncio.CancelledError, concurrent.futures.CancelledError):		
		bu.debug( str(len(results)) + ' replays, ' + str(len(playerstanks)) + ' player/tanks', readerID)
		return results, playerstanks, replays
	return None


async def read_replay_JSON(replay_json: dict, args : argparse.Namespace) -> dict:
	"""Parse replay JSON dict"""
	global REPLAY_I
	account_id = args.account_id
	url = args.url
	#db = args.db
	result = dict()
	try:
		# bu.debug(str(replay_json))
		if not wi.chk_JSON_replay(replay_json):
			bu.debug('Invalid replay')
			return None
		
		result['_id'] = replay_json['_id']
		replay_summary = replay_json['data']['summary']
		result['battle_start_timestamp'] = int(replay_summary['battle_start_timestamp'])
		# TBD... 
		protagonist = int(replay_summary['protagonist'])
		
		# For filtering replays per submitter
		result['protagonist'] = protagonist

		if account_id == None:
			account_id = protagonist
		elif protagonist != account_id:
			if account_id in replay_summary['enemies']:
				# switch the teams...
				if replay_summary['battle_result'] != 2:
					if replay_summary['battle_result'] == 0:
						replay_summary['battle_result'] = 1
					else: 
						replay_summary['battle_result'] = 0
				tmp = replay_summary['enemies']
				replay_summary['enemies'] = replay_summary['allies']
				replay_summary['allies'] = tmp
			elif account_id in replay_summary['allies']:
				pass 			
			else:		
				# looking for an account_id, but account_id not found in the teams => skipping the replay
				return None
		if url: 
			result['url'] = replay_json['data']['view_url']
				
		for key in replay_summary_flds:
			result[key] = replay_summary[key]
	
	except Exception as err:
		bu.error(exception=err)
		return None
	try:	
		#bu.debug('Part 2')
		result['allies'] = set()
		result['enemies'] = set()
		result['allies_survived']  = 0 	# for steamroller stats
		result['enemies_survived']  = 0	# for steamroller stats
		btl_duration = 0
		btl_tier = 0
		protagonist_tank  = None
		for player in replay_summary['details']:
			btl_duration = max(btl_duration, player['time_alive'])
			player_tank_tier = wg.get_tank_data(player['vehicle_descr'], 'tier')
			btl_tier = max(btl_tier, player_tank_tier)

			if player['dbid'] == account_id:
				# player itself is not put in results['allies']
				tmp = dict()
				tmp['account_id'] 	= account_id
				tank_id 			= player['vehicle_descr']
				tmp['tank_id'] 		= tank_id
				tmp['tank_tier'] 	= player_tank_tier
				tmp['tank_name']	= wg.get_tank_name(tmp['tank_id'])
				tmp['tank_type']	= wg.get_tank_type_id(tmp['tank_id'])
				tmp['tank_nation']	= wg.get_tank_nation_id(tmp['tank_id'])
				tmp['is_premium']	= int(wg.is_premium(tmp['tank_id']))
				platoon_ndx 		= player['squad_index']
				tmp['squad_index'] 	= platoon_ndx
				tmp['in_platoon']	= 0 if platoon_ndx == None else 1
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
				
				if player['dbid'] in replay_summary['allies']: 
					result['allies'].add(get_stat_id(tmp_account_id, tmp_tank_id, tmp_battletime))
					if survived:
						result['allies_survived'] += 1
				else:
					result['enemies'].add(get_stat_id(tmp_account_id, tmp_tank_id, tmp_battletime))
					if survived:
						result['enemies_survived'] += 1

		result['battle_duration'] = btl_duration
		## Rather use 'player' than incomprehensible 'protagonist'...		
		result['player'] = get_stat_id(account_id, tank_id, result['battle_start_timestamp'])
		bu.debug('Player stats_id: ' + result['player'])
		# remove platoon buddy from stats 			
		if result['squad_index'] != None:
			for player in replay_summary['details']:
				bu.debug(str(player))
				if (player['squad_index'] == result['squad_index']) and (player['dbid'] in replay_summary['allies']) and (player['dbid'] != account_id):
					# platoon buddy found 
					tmp_account_id 	= player['dbid']
					tmp_tank_id 	= player['vehicle_descr']
					#tmp_tank_tier 	= str(wg.get_tank_tier(tmp_tank_id))
					tmp_battletime	= result['battle_start_timestamp']
					# platoon buddy removed from stats 
					result['allies'].remove(get_stat_id(tmp_account_id, tmp_tank_id, tmp_battletime))
					break
		for ratio in BattleCategory.get_result_fields_ratio(all=True):
			result[ratio] = BattleCategory.calc_ratio(ratio, result)

		result['alive'] = min(result['time_alive'] / result['battle_duration'], 1)  
		result['battle_tier'] = btl_tier
		result['top_tier'] = 1 if (result['tank_tier'] == btl_tier) else 0
		result['win'] = 1 if result['battle_result'] == 1 else 0
		REPLAY_I += 1
		result['battle_i'] 	= REPLAY_I
		result['battle'] 	= str(REPLAY_I) + ': ' + replay_summary['vehicle'] + ' @' + replay_summary['map_name']
		bu.debug(str(result))
		return result
	except KeyError as err:
		bu.warning('Invalid replay. Key not found', err)
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


def get_stat_id(account_id: int, tank_id: int, battle_time: int) -> str:
	return ':'.join(map(str, [account_id, tank_id, battle_time ]))


def get_stat_id_tank_tier(stat_id_str: str) -> str:
	"""Return stat_id = account_id:tank_tier:battletime"""
	try:
		stat_id 	= str2ints(stat_id_str)
		account_id	= stat_id[0]
		tank_tier 	= wg.get_tank_tier(stat_id[1])
		battle_time = (stat_id[2] // BATTLE_TIME_BUCKET) * BATTLE_TIME_BUCKET
		return get_stat_id(account_id, tank_tier, battle_time)
	except Exception as err:
		bu.error('Stats_id: ' + stat_id_str, exception=err)
	return None


def get_stat_id_tank(stat_id_str: str) -> str:
	"""Return stat_id = account_id:tank_id:battletime"""
	try:
		stat_id 	= str2ints(stat_id_str)
		account_id	= stat_id[0]
		tank_id 	= stat_id[1]
		battle_time = (stat_id[2] // BATTLE_TIME_BUCKET) * BATTLE_TIME_BUCKET
		return get_stat_id(account_id, tank_id, battle_time)		
	except Exception as err:
		bu.error('Stats_id: ' + stat_id_str, exception=err)
	return None

def get_stat_id_player(stat_id_str: str) -> str:
	"""Return stat_id = account_id:battletime"""
	try:
		stat_id 	= str2ints(stat_id_str)
		battle_time = (stat_id[2] // BATTLE_TIME_BUCKET) * BATTLE_TIME_BUCKET
		return ':'.join(map(str, [ stat_id[0], battle_time ]))
	except Exception as err:
		bu.error('Stats_id: ' + stat_id_str, exception=err)		
	return None


def get_account_id_f_stat_id(stat_id_str: str) -> int:
	"""get account_id from a stat_id"""
	try:
		stat_id 	= str2ints(stat_id_str)
		return stat_id[0]
	except Exception as err:
		bu.error('Stats_id: ' + stat_id_str, exception=err)
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
