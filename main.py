from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from urllib.request import urlopen
from pymongo import MongoClient
from prophet import Prophet
from datetime import date
import requests
import pymongo
import pandas as pd
import os

# url = "https://my.meteoblue.com/packages/basic-day?apikey=kvNdbal3sQ5eyKOX&lat=13.55&lon=-88.8167&asl=481&format=json"

# location = "Bengaluru"
# url = "https://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=6052652525c04c6b88b74541222512&q={}&format=json&date={}&enddate={}&includelocation=yes&tp=24".format(location, start_date, end_date)


# response = requests.request("GET", url, headers=headers, data=payload)

def data_into_mongo(location, client):
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
            response = requests.request("GET", url)
            data = response.json()
            for k in data["data"]["weather"]:
                col.insert_one(k)

def retrieve_data_from_mongo(spark):
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/Real_Time_Weather.{}".format(loc)).load()
    
    df = df.drop("_id")
    
    hourly_data = df.select("date","hourly.tempC","hourly.windspeedKmph", "hourly.precipMM", "hourly.humidity", "hourly.pressure").toPandas()
    
    return hourly_data

def preprocess_data(data_pdf):
    data_pdf["Temp"] = data_pdf["tempC"].apply(pd.Series)
    data_pdf["Rain"] = data_pdf["precipMM"].apply(pd.Series)
    data_pdf["Humidity"] = data_pdf["humidity"].apply(pd.Series)
    data_pdf["WindSpeed"] = data_pdf["windspeedKmph"].apply(pd.Series)
    data_pdf["Pressure"] = data_pdf["pressure"].apply(pd.Series)
    
    data_pdf = data_pdf.drop(['tempC', 'precipMM', "humidity","windspeedKmph", "pressure"], axis=1)
    
    convert_dict = {'Temp': int,
                'Rain': float,
                'Humidity':int,
                'WindSpeed': int,
                'Pressure': int
                }
    data_pdf = data_pdf.astype(convert_dict)
    data_pdf['date'] = pd.DatetimeIndex(data_pdf['date'])
    
    return data_pdf

def process_for_prophet(data_df, var):
    df = data_df[["date",var]]
    df = df.rename(columns={'date': 'ds',
                        var: 'y'})
    return df

def get_future_df():
    today = date.today()
    d1 = int(today.strftime("%d"))
    future = list()
    for i in range(d1, d1+8):
        dat = '2023-01-%02d' % i
        future.append([dat])
    future = pd.DataFrame(future)
    future.columns = ['ds']
    future['ds']= pd.to_datetime(future['ds'])

def prophet_model_training(df, future):
    prophet_model= Prophet(interval_width=0.90)
    prophet_model.fit(df)
    forecast = prophet_model.predict(future)
    return prophet_model,forecast

payload={}
headers = {}

client = MongoClient("mongodb://localhost:27017/")        
db = client['Real_Time_Weather']
collection = db["sample"]
loc = input("Enter the location: ")

try:
    if (db.validate_collection(f"{loc}")):  # Try to validate a collection
        print("Collection exists")
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:///C:/Users/Ansuman/Desktop/MiniProject/mongo-spark-connector_2.12-3.0.1-assembly.jar pyspark-shell'

    spark = SparkSession.builder \
        .appName("MyApp") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
        .config("spark.mongodb.output.collection", collection) \
        .getOrCreate()
    
    data_pdf = retrieve_data_from_mongo(spark)
    data_df = preprocess_data(data_pdf)
    df = process_for_prophet(data_df, "Temp")
    future = get_future_df()
    model, fore = prophet_model_training(df, future)
        
        
except pymongo.errors.OperationFailure:  # If the collection doesn't exist
    print("Collecting History Data")
    data_into_mongo(loc, client)



