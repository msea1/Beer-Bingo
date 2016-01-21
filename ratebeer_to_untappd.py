import requests, beer_api, random

db = None
config = None
cursor = None

def load_rb_beers():
    BEERS = {}
    sql = 'SELECT brewer_name, brewer_id, beer_name, beer_id, my_score FROM rb_beers WHERE in_untappd = 0 AND should_skip = 0'
    cursor.execute(sql)
    for row in cursor.fetchall():
        brewer_obj = beer_api.Brewer(row[1], None, str(row[0]))
        beer_obj = beer_api.Beer(row[3], None, row[2], brewer_obj, None)
        rb_score = (round(float(row[4])*2))/2
        beer_obj.add_my_score(rb_score)
        BEERS[str(row[2])] = beer_obj
    return BEERS


def update_rb_list_with_checkin(beer_id):
    if beer_id:
        sql = 'UPDATE rb_beers SET in_untappd = 1 WHERE beer_id = %s' % beer_id
        cursor.execute(sql)


def update_rb_list_with_skip(beer_id):
    if beer_id:
        sql = 'UPDATE rb_beers SET should_skip = 1 WHERE beer_id = %s' % beer_id
        cursor.execute(sql)
    

def get_access_token():
    return config['untappd']['token']


def checkin(beer_id, rating):
    post_data = {
        'gmt_offset':'-8',
        'timezone':'PST',
        'bid':str(beer_id),
        'rating':str(rating)
    }
    url = 'https://api.untappd.com/v4/checkin/add?access_token=%s' % get_access_token()
    req = requests.post(url, data=post_data)
    json = req.json()
    return json


def search_for_beer(search_str):
    # TODO: put a timeout around the request
    req = requests.get('https://api.untappd.com/v4/search/beer?'+
        'access_token='+get_access_token()+'&'+
        'q='+str(search_str)+'&'+
        'limit=5')
    json = req.json()
    if json and json['response'] and json['response']['beers'] and json['response']['beers']['items']:
        return json
    else:
        print('Problem searching for: %s') % search_str
        #print('Found %s') % json
        return False


def pick_beer(search_str, json, rating, rb_id):
    beers_returned = len(json['response']['beers']['items'])
    ID_DICT = {}
    input_str = 'Searching for: %s and found:\n' % (search_str)
    input_str+='    0. None of the below\n'
    for i in range(beers_returned):
        beer_json = json['response']['beers']['items'][i]
        beer_id = beer_json['beer']['bid']
        checkins = beer_json['checkin_count']
        brewery_name = beer_json['brewery']['brewery_name']
        brewery_name = beer_api.english_only(brewery_name)
        beer_name = beer_json['beer']['beer_name']
        beer_name = beer_api.english_only(beer_name)
        have_had = beer_json['have_had']
        display_str = "%s %s -- %s checkins" % (brewery_name, beer_name, checkins)
        ID_DICT[i] = (beer_id, have_had)
        input_str+= '    %s. %s\n' % ((i+1), display_str)
    choice = input(input_str)
    if not choice:
        offer_skip(search_str, rating, rb_id)
        return (False, False)
    return ID_DICT[choice-1]


def offer_skip(search_str, rating, rb_id):
    input_str = 'Could not locate %s in untappd. It deserves a rating of %s.\n' % (search_str, rating)
    input_str+='Should we skip?\n'
    input_str+='    0. No\n'
    input_str+='    1. Yes\n'
    input_str+='    2. Manually Entered\n'
    choice = input(input_str)
    if choice == 1:
        update_rb_list_with_skip(rb_id)
    if choice == 2:
        update_rb_list_with_checkin(rb_id)


def main():
    beer_dict = load_rb_beers()
    counter = 0
    keys = beer_dict.keys()
    random.shuffle(keys)
    for key in keys:
        beer = beer_dict[key]
        rating = beer.my_score
        if not rating:
            continue
        brewer = beer_api.prune_brewer_name(beer.brewer.name)
        # TODO strip style from beer name initially
        search_str = "%s %s" % (brewer, beer.name)
        search_json = search_for_beer(search_str)
        counter+=1
        if not search_json:
            search_str = beer.name
            search_json = search_for_beer(search_str)
            counter+=1
        if not search_json:
            offer_skip(search_str, rating, beer.id)
        if search_json:        
            (ut_beer_id, have_had) = pick_beer(search_str, search_json, rating, beer.id)
            if have_had:
                print('Already checked into this beer')
                update_rb_list_with_checkin(beer.id)
                continue
            if ut_beer_id:
                checkin_resp = checkin(ut_beer_id, rating)
                if checkin_resp:
                    print('Successfully checked in to %s' % search_str)
                    update_rb_list_with_checkin(beer.id)
                    counter+=1
                    if counter >= 100:
                        return   


config = beer_api.read_config()
db = beer_api.init_db()  
cursor = db.cursor()
main()
