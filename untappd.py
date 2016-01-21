import requests
import beer_api
import pdb
from bs4 import BeautifulSoup

STYLES = {}
BREWERS = {}
BEERS = {}
db = None
config = None


def load_styles():
    user_name = config['untappd']['user']
    req = requests.get("https://untappd.com/user/%s/beers" % user_name)
    raw_src = req.text
    soup = BeautifulSoup(raw_src)
    styles = []
    pdb.set_trace()
    for option in soup.find(id="style_picker").find_all('option')[1:]:
        text = option.get_text()
        text = text[:text.find('(')]
        textE = beer_api.english_only(text)
        styles.append(textE)
    for style in styles:
        beer_api.find_style(table='ut_styles', style=style.strip())

    
def record_rating(beer):
    # does rating already exist in DB?
    cursor = db.cursor()
    sql = "SELECT * FROM ut_beers WHERE (beer_id=%s OR alias_id=%s) AND brewer_id=%s AND style_id=%s" % (beer.id, beer.id, beer.brewer.id, beer.style.id)
    cursor.execute(sql)
    rows = cursor.fetchall()
    if len(rows) > 1:  # error!!
        return -1
    if len(rows) == 1:
        sql = "UPDATE ut_beers SET my_score=%s, rb_score=%s WHERE beer_id=%s AND brewer_id=%s AND style_id=%s" % (beer.my_score, beer.site_score, beer.id, beer.brewer.id, beer.style.id)
        cursor.execute(sql)
        cursor.close()
        db.commit()
        return True
    # insert into DB
    sql = "INSERT IGNORE INTO ut_beers (beer_id, brewer_id, style_id, rb_score, my_score) VALUES (%s, %s, %s, %s, %s)" % (beer.id, beer.brewer_id, beer.style_id, beer.rb_score, beer.my_score)
    if cursor.execute(sql):
        cursor.close()
        db.commit()
        return True
    return False


def get_ratings():
    offset = 0
    while offset >= 0:
        user_name = config['untappd']['user']
        client_id = config['untappd']['client_id']
        client_secret = config['untappd']['client_secret']
        req = requests.get('https://api.untappd.com/v4/user/beers/%s?'+
            'client_id=%s&'+
            'client_secret=%s&'+
            'offset='+str(offset)+'&'+
            'limit=50' % (user_name, client_id, client_secret))
        json = req.json() 
        
        if json and json['response'] and json['response']['beers'] and json['response']['beers']['items']:
            beers_returned = len(json['response']['beers']['items'])
        else:
            offset = -1
            continue
        for i in range(beers_returned):
            try:
                beer_json = json['response']['beers']['items'][i]
                
                brewery_name = beer_api.english_only(beer_json['brewery']['brewery_name'])
                beer_style = beer_api.english_only(beer_json['beer']['beer_style'])

                brewer = beer_api.find_brewer("ut_brewers", brewery_name.decode())
                style = beer_api.find_style("ut_styles", beer_style.decode())
                
                beer_name = beer_api.english_only(beer_json['beer']['beer_name'])
                my_rating = beer_json['rating_score']
                net_rating = beer_json['beer']['rating_score']

                beer = beer_api.find_beer("ut_beers", beer_name.decode(), brewer, style)
                beer.add_my_score(float(my_rating))
                beer.add_site_score(float(net_rating))
                record_rating(beer)
            except UnicodeEncodeError:
                print(beer_json)

        offset += beers_returned


config = beer_api.read_config()
db = beer_api.init_db()  
STYLES, BREWERS, BEERS = beer_api.build_caches("ut")
#load_styles()
get_ratings()
