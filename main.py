from dotenv import load_dotenv, find_dotenv
import os 
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"""mongodb+srv://Kerim23:{password}@mongodb.9ekua.mongodb.net/?retryWrites=true&w=majority"""


client = MongoClient(connection_string)


dbs = client.list_database_names()
#print(dbs)
mongodb = client.first_mongodb
collections = mongodb.list_collection_names()
#print(collections)

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
#insert_test_doc()

#output is 6290cbb07df5e6e1c24d37b3 == insert id which is a bson object id


production = client.production
person_collection = production.person_collection
#mongodb will auto create client which is db and production collection

def create_documents():
    first_names = ["Kerim", "Esteban", "Ricardo", "Renzo", "Eric", "Ali", "Mike", "Dan", "Brandon", "Deigo"]
    last_names = ["Sever", "Roldan", "Regatao", "Santamaria", "Moreales", "Timur", "DiGangi", "DiGangi", "Furtado", "Belo"]
    ages = [22, 21, 21, 23, 23, 23, 22, 23, 22, 22, 22]

    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)
        #person_collection.insert_one(doc)

    person_collection.insert_many(docs)

#create_documents()

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



def count_all_people():
    count = person_collection.count_documents(filter={})
    print("Number of people", count)

#count_all_people()



def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    #need to do this for all id into obj id with bson
    var_id = ObjectId(person_id)
    person = person_collection.find_one({"_id": var_id})
    printer.pprint(person)

#get_person_by_id("6290e5073357146dc2401a23")


def get_age_range(min_age, max_age):
    query = {"$and": [
                {"age": {"$gte": min_age}}, #syntax "age" is the field and {"$gte": min_age} is the query
                {"age": {"$lte": max_age}} #$lte = less than equal to  $gte = greater than equal to $ly = less than
            ]}

    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

#get_age_range(20, 21)


def project_columns():
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)

#project_columns() 


#this is the updates portion of the code, before were just creating queries

def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    var_id = ObjectId(person_id)

    all_updates = {
        "$set": {"new_field": True},
        "$inc": {"age": 1},
        "$rename": {"first_name": "first", "last_name": "last"}
    }

    person_collection.update_one({"_id": var_id}, all_updates)
    
    #person_collection.update_one({"_id": var_id}, {"$unset": {"new_field": ""}})


#update_person_by_id("6290e5073357146dc2401a25")



def replace_one(person_id):
    from bson.objectid import ObjectId
    var_id = ObjectId(person_id)

    new_doc = {
        "first_name": "new first name",
        "last_name": "new last name",
        "age": 100
    }

    person_collection.replace_one({"_id": var_id}, new_doc)


#replace_one("6290e5073357146dc2401a27")




def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    var_id = ObjectId(person_id)
    person_collection.delete_one({"_id": var_id})
    person_collection.delete_many({"_id": var_id})#if you leave {} empty itll delete everything

#delete_doc_by_id("6290e5073357146dc2401a2b")
#this removed belo


#------------------------------------------------------------------

address = {
    "_id": "2343247239847",
    "street": "Albert street",
    "number": 16,
    "city": "North Arlington",
    "country": "United States",
    "zip": "07031",
    "owner_id": "6290e5073357146dc2401a24" #this is the same as the person _id indicating that it belongs to that person
}


# person = {
#     "_id": "6345435345435",
#     "first_name": "James",
#     "address": {#you can make a list of address by using the [] around {} like this [{}]
#         "_id": "2343247239847",
#         "street": "Albert street",
#         "number": 16,
#         "city": "North Arlington",
#         "country": "United States",
#         "zip": "07031"
#     }
# }


def add_address_embed(person_id, address):
    from bson.objectid import ObjectId
    var_id = ObjectId(person_id)

    person_collection.update_one({"_id": var_id}, {"$addToSet": {'addresses': address}})

#add_address_embed("6290e5073357146dc2401a24", address)




def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId
    var_id = ObjectId(person_id)

    address = address.copy()
    address["owner_id"] = person_id

    address_collection = production.address
    address_collection.insert_one(address)

add_address_relationship("6290e5073357146dc2401a24", address)