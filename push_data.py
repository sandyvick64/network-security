import os
import sys
import json
from dotenv import load_dotenv
import certifi
import pandas as pd
import numpy as np
import pymongo
from networksecurity.exceptions.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

load_dotenv()

mongo_db_url = os.environ['MONGO_DB_URL']

# trusted certificate store
ca = certifi.where()

class NetworkDataExtractor():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def cv_to_json_converter(self, file_path):
        try:
            data=pd.read_csv(file_path)
            data.reset_index(drop=True,inplace=True)
            records=list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkDataExtractor(e,sys)
    
    def insert_data_mongodb(self,records,database,collection):
        try:
            self.database=database
            self.collection=collection
            self.records=records

            self.mongo_client=pymongo.MongoClient(mongo_db_url)
            self.database = self.mongo_client[self.database]
            
            self.collection=self.database[self.collection]
            self.collection.insert_many(self.records)
            return(len(self.records))
        except Exception as e:
            raise NetworkSecurityException(e,sys)

if __name__ == "__main__":
    FILE_PATH = r"D:\VSrepo\udemy_mlops\NetworkSecurity\network_data\phisingData.csv"
    DATABSE = "sandyvick64"
    collecton = "NetworkData"

    net_obj = NetworkDataExtractor()
    records = net_obj.cv_to_json_converter(file_path=FILE_PATH)

    no_of_records = net_obj.insert_data_mongodb(records=records,database=DATABSE,collection=collecton)

    print(f"total no of records added in Mongo DB {no_of_records}")

