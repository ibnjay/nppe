"""
# To use Google Lib for python

pip install -U googlemaps
import googlemaps
from datetime import datetime

gmaps = googlemaps.Client(key=inputKey)
geocode_result = gmaps.geocode('1600 Amphitheatre Parkway, Mountain View, CA')
loc = geocode_result[0].get('geometry').get('location')
loc.get('lat')
loc.get('lng')

"""

key = 'yourAPIKeys'
import requests
from pyspark.sql.functions import lit, concat

def get_lat(address):
    apiKey = key
    url = ('https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'
           .format(address.replace(' ','+'), apiKey))
    print(url)
    try:
        response = requests.get(url)
        resp_json_payload = response.json()
        lat = resp_json_payload['results'][0]['geometry']['location']['lat']
        lng = resp_json_payload['results'][0]['geometry']['location']['lng']
    except:
        print('ERROR: {}'.format(address))
        lat = 0
        lng = 0
    return lat

def get_lng(address):
    apiKey = key
    url = ('https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'
           .format(address.replace(' ','+'), apiKey))
    print(url)
    try:
        response = requests.get(url)
        resp_json_payload = response.json()
        lat = resp_json_payload['results'][0]['geometry']['location']['lat']
        lng = resp_json_payload['results'][0]['geometry']['location']['lng']
    except:
        print('ERROR: {}'.format(address))
        lat = 0
        lng = 0
    return lng


practice = spark.read.option("header","true").csv("gs://jay-storage-cli/cms/address/practice/*.csv")
practice = practice.limit(100)
practice.show()


practice = practice.withColumn('joined_address', concat(practice['first_line'],lit(', '), practice['city_name'],  lit(', '), practice['state_name'],   lit(', '), practice['postal_code']))


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
lat_udf = udf(get_lat, StringType())
lng_udf = udf(get_lng, StringType())

practice = practice.withColumn("latitude", lat_udf(practice["joined_address"])).withColumn("longitude", lng_udf(practice["joined_address"]))

practice_geo = practice.drop("joined_address")
practice_geo.write.option("header","true").csv("gs://jay-storage-cli/cms/address/practice_geo/")





"""

# Others things that i tried. 


import requests
def get_lat_lng(address):
    apiKey = keys
    url = ('https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'
           .format(address.replace(' ','+'), apiKey))
    print(url)
    try:
        response = requests.get(url)
        resp_json_payload = response.json()
        lat = resp_json_payload['results'][0]['geometry']['location']['lat']
        lng = resp_json_payload['results'][0]['geometry']['location']['lng']
    except:
        print('ERROR: {}'.format(address))
        lat = 0
        lng = 0
    return lat, lng



practice.select('npi', "joined_address", lat_udf("joined_address").alias("latitude"))

spark.udf.register("get_lat_lng", get_lat_lng)
practice.select('npi', "joined_address", get_lat_lng("joined_address")[0].alias("lat"), get_lat_lng("joined_address")[1].alias("lng"))

practice.withColumn("lat_lng", get_lat_lng(practice["joined_address"]))


out = practice.rdd.map(lambda x: x.joined_address)
spark.createDataFrame(out).collect()

"""
