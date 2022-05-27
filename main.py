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

def insert_test_doc():
    collection = mongodb.first_collection
    #adding changes after the . is the name of the collection
    test_document = {
        "name": "Kerim",
        "type": "Test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print (inserted_id)
insert_test_doc()

#output is 6290cbb07df5e6e1c24d37b3 == insert id which is a bson object id


production = client.production
person_collection = production.person_collection
#mongodb will auto create client which is db and production collection

def create_documents():
    first_names = ["Kerim", "Esteban", "Ricardo", "Renzo", "Eric", "Ali", "Mike", "Dan", "Brandon", "Deigo"]
    last_names = ["Sever", "Roldan", "Regatao", "Santamaria", "Moreales", "Timur", "DiGangi", "DiGangi", "Furtado", "Belo"]
    ages = [22, 21, 21, 23, 23, 21, 22, 23, 22, 22, 22]

    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)
        #person_collection.insert_one(doc)

    person_collection.insert_many(docs)

create_documents()

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()
    #for print(people) you get pymongo cursor 

    for person in people:
        printer.pprint(person)


#find_all_people()

#output in documents and can be treated as dictionarys 

def find_kerim():
    kerim = person_collection.find_one({"first_name": "Kerim", "last_name": "Sever"})
    printer.pprint(kerim)

#find_kerim()

