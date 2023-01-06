import requests
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from urllib.request import urlopen
from pymongo import MongoClient
import pymongo
import pandas as pd
import os


# url = "https://my.meteoblue.com/packages/basic-day?apikey=kvNdbal3sQ5eyKOX&lat=13.55&lon=-88.8167&asl=481&format=json"

# location = "Bengaluru"
# url = "https://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=6052652525c04c6b88b74541222512&q={}&format=json&date={}&enddate={}&includelocation=yes&tp=24".format(location, start_date, end_date)


# response = requests.request("GET", url, headers=headers, data=payload)

def data_into_mongo(location):
    days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    start_date = "2010-01-01"
    db = client['Real_Time_Weather']
    col = db[location]
    
    for year in [2019, 2020, 2021]:
        j = 1
        for i in days:
            start_date = "{}-{}-01".format(year,j)
            end_date = "{}-{}-{}".format(year,j,i)
            j += 1
            
            url = "https://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=6052652525c04c6b88b74541222512&q={}&format=json&date={}&enddate={}&includelocation=yes&tp=24".format(location, start_date, end_date)
            response = requests.request("GET", url, headers=headers, data=payload)
            data = response.json()
            for k in data["data"]["weather"]:
                col.insert_one(k)
                print(k)

def preprocess_data():
    pass


payload={}
headers = {}

client = MongoClient("mongodb://localhost:27017/")        
db = client['Real_Time_Weather']
collection = db["sample"]
loc = input("Enter the location: ")

try:
    if (db.validate_collection(f"{loc}")):  # Try to validate a collection
        print("Collection exists")
        
except pymongo.errors.OperationFailure:  # If the collection doesn't exist
    print("Collecting History Data")
    data_into_mongo("Bengaluru")

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:///C:/Users/Ansuman/Desktop/MiniProject/mongo-spark-connector_2.12-3.0.1-assembly.jar pyspark-shell'

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
    .config("spark.mongodb.output.collection", collection) \
    .getOrCreate()
    
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/Real_Time_Weather.{loc}}").load()

df.show()

