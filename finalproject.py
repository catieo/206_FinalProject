import requests
import json
import yelp
import sqlite3

#secret values to authenticate to the Yelp Fusion API 
client_id = yelp.client_id
client_secret = yelp.client_secret
access_token = yelp.access_token

#set up authentication to Yelp
base_url = "https://api.yelp.com"
search_api_path = "/v3/businesses/search"
#COME BACK TO THIS AND REFORMAT STRING WITH EACH BUSINESS WHEN YOU MAKE THE REQUEST
reviews_api_path = "/v3/businesses/{}/reviews"

#caching pattern for search_results_cache
CACHE_FNAME = "search_results_cache.json"
try:
	cache_file = open(CACHE_FNAME,'r')
	cache_contents = cache_file.read()
	cache_file.close()
	CACHE_DICTION = json.loads(cache_contents)
except:
	CACHE_DICTION = {}

#caching pattern for reviews of different businesses
CACHE_FNAME_2 = "reviews_cache.json"
try:
	cache_file = open(CACHE_FNAME_2, 'r')
	cache_contents = cache_file.read()
	cache_file.close()
	CACHE_DICTION_2 = json.loads(cache_contents)
except:
	CACHE_DICTION_2 = {}

#function for making requests to the Yelp API 
#"path" will change depending on whether accessing Search or Reviews API 
def yelp_request(host, path, token, url_params=None):
	url = host + path 
	headers = {'Authorization' : 'Bearer %s' % token,}
	response = requests.request('GET', url, headers=headers, params=url_params)
	return response.json()

#function to query the Search API by a search term and location working with the caching pattern 
def search(term, location):
	url_params = {'term' : term.replace(' ', '+'), 'location' : location.replace(' ', '+'), 'limit' : 50}
	full_search_term = term + "_" + location
	if full_search_term in CACHE_DICTION:
		print("Data was in the cache")
		return CACHE_DICTION[full_search_term]
	else:
		print("Making a request for new data")
		results = yelp_request(base_url, search_api_path, access_token, url_params)
		CACHE_DICTION[full_search_term] = results
		dumped_json_cache = json.dumps(CACHE_DICTION)
		fw = open(CACHE_FNAME, "w")
		fw.write(dumped_json_cache)
		fw.close()
		return CACHE_DICTION[full_search_term]

results = search("brunch", "Chicago")
results2 = search("brunch", "Detroit")

#function to query the Reviews API by name of a business working with the caching pattern 
def get_reviews(business):
	if business in CACHE_DICTION_2:
		print("Data was in the cache")
		return CACHE_DICTION_2[business]
	else:
		print("Making a request for new data")
		results = yelp_request(base_url, reviews_api_path.format(business), access_token)
		CACHE_DICTION_2[business] = results
		dumped_json_cache = json.dumps(CACHE_DICTION_2)
		fw = open(CACHE_FNAME_2, "w")
		fw.write(dumped_json_cache)
		fw.close()
		return CACHE_DICTION_2[business]

#create database - table for search results, table for reviews 
conn = sqlite3.connect("yelp.sqlite")
cur = conn.cursor() 

#Create Businesses table 
cur.execute('DROP TABLE IF EXISTS Businesses')
cur.execute('CREATE TABLE Businesses (id TEXT, rating FLOAT, num_reviews INTEGER, price TEXT, latitude FLOAT, longitude FLOAT)')
#Create Reviews table 
#bus_id is a foreign key 
cur.execute('DROP TABLE IF EXISTS Reviews')
cur.execute('CREATE TABLE Reviews (id INTEGER PRIMARY KEY, bus_id TEXT, time_posted TIMESTAMP, ind_rating INTEGER, review_text TEXT, user_posted TEXT)')

#for loop to insert data into tables
for business in results["businesses"]:
	tup = business["id"], business["rating"], business["review_count"], business["price"], business["coordinates"]["latitude"], business["coordinates"]["longitude"]
	#inserts business into Businesses table 
	cur.execute('INSERT INTO Businesses (id, rating, num_reviews, price, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)', tup)
	#queries Reviews API and then inserts reviews for each Business into Reviews table 
	reviews = get_reviews(business["id"])
	for review in reviews["reviews"]:
		rtup = business["id"], review["time_created"], review["rating"], review["text"], review["user"]["name"]
		cur.execute('INSERT INTO Reviews (bus_id, time_posted, ind_rating, review_text, user_posted) VALUES (?, ?, ?, ?, ?)', rtup)

for business in results2["businesses"]:
	#inserts business into BUsinesses table
	tup = business["id"], business["rating"], business["review_count"], business["price"], business["coordinates"]["latitude"], business["coordinates"]["longitude"]
	cur.execute('INSERT INTO Businesses (id, rating, num_reviews, price, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)', tup)
	#queries Reviews API and then inserts reviews for each Business into Reviews table 
	reviews = get_reviews(business["id"])
	for review in reviews["reviews"]:
		rtup = business["id"], review["time_created"], review["rating"], review["text"], review["user"]["name"]
		cur.execute('INSERT INTO Reviews (bus_id, time_posted, ind_rating, review_text, user_posted) VALUES (?, ?, ?, ?, ?)', rtup)

#join Business and Review tables according to bus_id key 
#save in a list of tuples for now, maybe make another table? 
joined_data = []
for row in cur.execute('SELECT * FROM Businesses JOIN Reviews ON Businesses.id = Reviews.bus_id'):
	joined_data.append(row)
print(joined_data[0], joined_data[1], joined_data[2], joined_data[3])

conn.commit()
cur.close()



	









