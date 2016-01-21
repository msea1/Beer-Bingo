import requests
import beer_api
from bs4 import BeautifulSoup

STYLES = {}
BREWERS = {}
BEERS = {}
db = None
config = None

    
def get_ratings():
    userid = config['ratebeer']['userid']
    req = requests.get("http://www.ratebeer.com/user/%s/ratings/" % userid)
    grab_the_table(req)
    for page in range(1,30):
        req = requests.get("http://www.ratebeer.com/user/%s/ratings/%s/5/" % (userid, page))
        if req:
            grab_the_table(req)
    
    
def grab_the_table(req):
    #req.encoding = 'mac_latin2'
    raw_src = req.text
    soup = BeautifulSoup(raw_src)
    table = soup.find("table", attrs={"class": "maintable linkhead"})
    #grab_headers(table)
    grab_the_data(table)


def grab_headers(src):
    # Beer, Brewer, Style, RB_Score, My-Score, Date
    headers = [str(th.get_text().strip()) for th in src.find_all("td", attrs={"class": "statsTableHeader"})]


def grab_the_data(table):
    if table:
        for row in table.find_all("tr")[1:]:
            # find all <td> in each <tr>, skip the first, get the text, strip it, make a list
            dataset = [td.get_text().strip().encode('ascii','ignore') for td in row.find_all("td")[1:]]
            #pdb.set_trace()
            style = beer_api.find_style("rb_styles", dataset[2].decode())
            if style is None:
                continue
            brewer = beer_api.find_brewer("rb_brewers", dataset[1].decode())
            if brewer is None:
                continue
            beer = beer_api.find_beer("rb_beers", dataset[0].decode(), brewer, style)
            if beer is None:
                continue
            beer.add_site_score(float(dataset[3].decode()))
            beer.add_my_score(float(dataset[4].decode()))
            record_rating(beer)

    
def record_rating(beer):
    # does rating already exist in DB?
    cursor = db.cursor()
    sql = "SELECT * FROM rb_beers WHERE (beer_id=%s OR alias_id=%s) AND brewer_id=%s AND style_id=%s" % (beer.id, beer.id, beer.brewer.id, beer.style.id)
    cursor.execute(sql)
    rows = cursor.fetchall()
    if len(rows) > 1:  # error!!
        return -1
    if len(rows) == 1:
        sql = "UPDATE rb_beers SET my_score=%s, rb_score=%s WHERE beer_id=%s AND brewer_id=%s AND style_id=%s" % (beer.my_score, beer.site_score, beer.id, beer.brewer.id, beer.style.id)
        cursor.execute(sql)
        cursor.close()
        db.commit()
        return True
    # insert into DB
    sql = "INSERT IGNORE INTO rb_beers (beer_id, brewer_id, style_id, rb_score, my_score) VALUES (%s, %s, %s, %s, %s)" % (beer.id, beer.brewer_id, beer.style_id, beer.site_score, beer.my_score)
    if cursor.execute(sql):
        cursor.close()
        db.commit()
        return True
    return False
   

config = beer_api.read_config()
db = beer_api.init_db()   
STYLES, BREWERS, BEERS = beer_api.build_caches("rb")
get_ratings()
