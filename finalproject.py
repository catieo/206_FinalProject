import requests
import json
import yelp
import sqlite3
import plotly
import plotly.plotly as py 
from plotly.graph_objs import * 

#secret values to authenticate to the Yelp Fusion API 
client_id = yelp.client_id
client_secret = yelp.client_secret
access_token = yelp.access_token

#secret values for Plotly 
plotly.tools.set_credentials_file(username=yelp.plotly_username, api_key=yelp.plotly_api_key)

#set up authentication to Yelp
base_url = "https://api.yelp.com"
search_api_path = "/v3/businesses/search"
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
joined_data = []
for row in cur.execute('SELECT Businesses.id, Reviews.time_posted, Businesses.rating FROM Businesses JOIN Reviews ON Businesses.id = Reviews.bus_id'):
	joined_data.append(row)

conn.commit()
cur.close()

#Plotly visualization
#create lists of reviews by hour of the day 
possible_hours = []
num_3_per_hour = []
num_35_per_hour = []
num_4_per_hour = []
num_45_per_hour = []
num_5_per_hour = []
temp3 = {}
temp35 = {}
temp4 = {}
temp45 = {}
temp5 = {}

for tup in joined_data:
	x = tup[1][11:13]
	if tup[2] == 3.0:
		if x not in temp3:
			temp3[x] = 1
		else:
			temp3[x] += 1 
		if x not in possible_hours:
			possible_hours.append(x)
	if tup[2] == 3.5:
		if x not in temp35:
			temp35[x] = 1
		else:
			temp35[x] += 1 
		if x not in possible_hours:
			possible_hours.append(x)
	if tup[2] == 4.0:
		if x not in temp4:
			temp4[x] = 1
		else:
			temp4[x] += 1 
		if x not in possible_hours:
			possible_hours.append(x)
	if tup[2] == 4.5:
		if x not in temp45:
			temp45[x] = 1
		else:
			temp45[x] += 1 
		if x not in possible_hours:
			possible_hours.append(x)
	if tup[2] == 5.0:
		if x not in temp5:
			temp5[x] = 1
		else:
			temp5[x] += 1 
		if x not in possible_hours:
			possible_hours.append(x)
for x in temp3:
	num_3_per_hour.append(temp3[x])
for x in temp35:
	num_35_per_hour.append(temp35[x])
for x in temp4:
	num_4_per_hour.append(temp4[x])
for x in temp45:
	num_45_per_hour.append(temp45[x])
for x in temp5:
	num_5_per_hour.append(temp5[x])

#draw graph
trace3 = graph_objs.Bar(x=possible_hours, y=num_3_per_hour, name='Rating 3.0')
trace35 = graph_objs.Bar(x=possible_hours, y=num_35_per_hour, name='Rating 3.5')
trace4 = graph_objs.Bar(x=possible_hours, y=num_4_per_hour, name='Rating 4.0')
trace45 = graph_objs.Bar(x=possible_hours, y=num_45_per_hour, name='Rating 4.5')
trace5 = graph_objs.Bar(x=possible_hours, y=num_5_per_hour, name='Rating 5.0')
data = [trace3,trace35,trace4,trace45,trace5]
layout = graph_objs.Layout(barmode = 'group', title='Different Ratings Given Per Hour')
fig = graph_objs.Figure(data=data, layout=layout)
py.plot(fig, filename='ratings_per_hour')




