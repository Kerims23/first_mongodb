from dotenv import load_dotenv, find_dotenv
import os 
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"""mongodb+srv://Kerim23:{password}@mongodb.9ekua.mongodb.net/?retryWrites=true&w=majority"""


client = MongoClient(connection_string)


dbs = client.list_database_names()
print(dbs)
mongodb = client.first_mongodb
collections = mongodb.list_collection_names()
print(collections)

#differences between MongoDb and SQL/others
#Others are usually RDBMS that use tables and use SQL
#Mongodb as a nosql stores unstructered data in json format 
#frequent updates and more flexible and faster with scalability!
#Database > colleciton > Documents
#if building db for library collections would be people, book, rentals with documents inside
# in book colleciton you would have data for it in a field value pair like a dictionary
#mongodb uses BSON
