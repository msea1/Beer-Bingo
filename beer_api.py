import pymysql
import unicodedata
import configparser
import os

STYLES = {}
BREWERS = {}
BEERS = {}
db = None
config = None


class Style(object):

    def __init__(self, id_key, alias, name):
        self.id = id_key
        self.alias = alias
        self.name = name


class Brewer(object):

    def __init__(self, id_key, alias, name):
        self.id = id_key
        self.alias = alias
        self.name = name


class Beer(object):

    def __init__(self, id_key, alias, name, brewer, style):
        self.id = id_key
        self.alias = alias
        self.name = name
        self.brewer = brewer
        self.style = style
        self.site_score = None
        self.my_score = None
        self.remove_brewer_from_name()


    def add_site_score(self, score):
        self.site_score = score


    def add_my_score(self, score):
        self.my_score = score


    def remove_brewer_from_name(self):
        if self.brewer is None or self.name is None:
            return
        old_name = self.name
        brewer_name = self.brewer.name 
        self.name = old_name.replace(brewer_name, "")


def read_config():
    global config
    config = configparser.ConfigParser()
    configFile = os.path.join(os.path.dirname(__file__), "config.ini")
    config.read(configFile)
    return config


def init_db():
    global db
    user = config['mysql']['user']
    pwd = config['mysql']['pwd']
    host = config['mysql']['host']
    db_name = config['mysql']['db_name']
    db = pymysql.connect(host=host, passwd=pwd, user=user, db=db_name)
    return db


def english_only(item):
    if type(item) is not unicode:
        item = item.replace('\xc3\x83\xc2\xa4','a')
        item.decode("utf-8")
        item = item.replace(u"\xc3\xa4", "a")
    return unicodedata.normalize('NFKD', item).encode('ascii', 'ignore').strip().replace("'","")


def find_style(table, style):
    global STYLES
    style = english_only(style)
    if style in STYLES:
        return STYLES[style]
    else:
        sql = "INSERT INTO %s (%s) VALUES ('%s')" % (table, 'style_name', style)
        cursor = db.cursor()
        if cursor.execute('%s' % sql):
            new_id = cursor.lastrowid
            cursor.close()
            db.commit()
        if new_id:
            style_obj = Style(new_id, None, style)
            STYLES[style] = style_obj
            return style_obj
        return None

    
def find_brewer(table, brewer):
    global BREWERS
    brewer = english_only(brewer)
    if brewer in BREWERS:
        return BREWERS[brewer]
    else:
        sql = "INSERT INTO %s (%s) VALUES ('%s')" % (table, 'brewer_name', brewer)
        cursor = db.cursor()
        if cursor.execute('%s' % sql):
            new_id = cursor.lastrowid
            cursor.close()
            db.commit()
        if new_id:
            brewer_obj = Brewer(new_id, None, brewer)
            BREWERS[brewer] = brewer_obj
            return brewer_obj
        return None

    
def find_beer(table, beer, brewer_obj, style_obj):
    global BEERS
    beer = english_only(beer)
    beer_key = beer + '~' + str(brewer_obj.id) + '~' + str(style_obj.id)
    if beer_key in BEERS:
        return BEERS[beer_key]
    else:        
        beer_obj = Beer(None, None, beer, brewer_obj, style_obj)
        sql = "INSERT INTO %s (beer_name, brewer_id, brewer_name, style_id, style_name) VALUES ('%s',%s,'%s',%s,'%s')" % (table, beer, brewer_obj.id, brewer_obj.name, style_obj.id, style_obj.name)
        cursor = db.cursor()
        if cursor.execute('%s' % sql):
            new_id = cursor.lastrowid
            cursor.close()
            db.commit()  
        if new_id:
            beer_obj.id = new_id
            BEERS[beer_key] = beer_obj
            return beer_obj
        return None


def build_caches(table_prefix):
    global STYLES, BREWERS, BEERS
    cursor = db.cursor()
    cursor.execute('SELECT style_name, style_id, alias_id FROM %s_styles' % (table_prefix))
    for row in cursor.fetchall():
        style_obj = Style(row[1], row[2], str(row[0]))
        STYLES[str(row[0])] = style_obj
    
    cursor.execute('SELECT brewer_name, brewer_id, alias_id FROM %s_brewers' % (table_prefix))
    for row in cursor.fetchall():
        brewer_obj = Brewer(row[1], row[2], str(row[0]))
        BREWERS[str(row[0])] = brewer_obj
    
    cursor.execute('SELECT beer_name, beer_id, alias_id, brewer_name, style_name FROM %s_beers' % (table_prefix))
    for row in cursor.fetchall():
        beer = str(row[0])
        beer_id = row[1]
        alias_id = row[2]
        brewer = BREWERS[str(row[3])]  # TODO not tied to the brewer_table here
        style = STYLES[str(row[4])]
        beer_obj = Beer(beer_id, alias_id, beer, brewer, style)
        beer_key = beer + '~' + str(brewer.id) + '~' + str(style.id)
        BEERS[beer_key] = beer_obj
    
    cursor.close()
    return (STYLES, BREWERS, BEERS)


def _update_brewer_match(cursor, matched_id, base_id):
    cursor.execute('UPDATE brewers SET ut_brewer_id = %s WHERE rb_brewer_id = %s' % (matched_id, base_id))


def _insert_brewer_no_match(cursor, unfound_id):
    sql = "INSERT INTO brewers (ut_brewer_id) VALUES (%s)" % (unfound_id)
    cursor.execute('%s' % sql)
    db.commit()


def prune_brewer_name(brewery_name):
    token = brewery_name.find('Brewing')
    if token > -1:
        return brewery_name[0:token].strip()
    token = brewery_name.find('(')
    if token > -1:
        return brewery_name[0:token].strip()
    token = brewery_name.find('Company')
    if token > -1:
        return brewery_name[0:token].strip()
    return brewery_name


def build_brewers_dict():
    init_db()
    PRUNED_BREWERS = {}
    RB_BREWERS = {}
    UT_BREWERS = {}
    cursor = db.cursor()
    cursor.execute('TRUNCATE TABLE brewers')
    cursor.execute('INSERT INTO brewers (rb_brewer_id) SELECT brewer_id FROM rb_brewers')
    cursor.execute('SELECT brewer_name, brewer_id, alias_id FROM rb_brewers')
    for row in cursor.fetchall():
        brewer_obj = Brewer(row[1], row[2], str(row[0]))
        RB_BREWERS[str(row[0])] = brewer_obj
    cursor.execute('SELECT brewer_name, brewer_id, alias_id FROM ut_brewers')
    for row in cursor.fetchall():
        brewer_obj = Brewer(row[1], row[2], str(row[0]))
        UT_BREWERS[str(row[0])] = brewer_obj
    for key in RB_BREWERS.keys():
        brewer = RB_BREWERS[key]
        new_brewer = prune_brewer_name(key)
        PRUNED_BREWERS[new_brewer] = brewer.id
    for key in UT_BREWERS.keys():
        brewer = UT_BREWERS[key]
        new_brewer = prune_brewer_name(key)
        if new_brewer in PRUNED_BREWERS:
            _update_brewer_match(cursor, brewer.id, PRUNED_BREWERS[new_brewer])
        else:
            _insert_brewer_no_match(cursor, brewer.id)
