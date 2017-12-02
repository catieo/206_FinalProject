import requests
import json
import yelp

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

reviews = get_reviews("wildberry-pancakes-and-cafe-chicago-2")



	









