import json 
import zipfile
import pymongo
import datetime
import tqdm

client = pymongo.MongoClient('localhost', 27017)
mydb = client["Chap3_real"]
collection = mydb["application2"]

with zipfile.ZipFile('data/Chap3/authors.zip', 'r') as zip_ref:
    zip_ref.extract('authors.json')

with open('authors.json', 'r', encoding='utf-8') as file:
    data = json.load(file)
    file.close()

for entry in data:
    try:
        entry['_id'] = entry['_id'].pop('$oid')
    except Exception as e:
        print(e, entry)

collection.insert_many(data)

# 1) Create an index, explain your choice of key.
collection.create_index([('_id',1)])

# 2) What is the average length of "pmid_list"
sum([len(entry['pmid_list']) for entry in collection.find({'pmid_list':{'$ne':None}})]) / collection.count_documents({})

# 3) How many distinct affiliations are there ?
values = collection.aggregate([
    {"$unwind": "$oa04_affiliations"},  # Décompacte les documents pour accéder à chaque affiliation
    {"$group": {"_id": "$oa04_affiliations.AffiliationType"}}  # Regroupe par la clé "AffiliationType"
])

[entry['_id'] for entry in values]

# 4) Find authors with atleast one "COM" AffiliationType
com_authors = collection.find({"oa04_affiliations.AffiliationType": "COM"})
[entry for entry in com_authors]


# 5) How many authors switched the AffiliationType ?
pipeline = [
    {"$match": {"more_info.LastName": {"$exists": True}}},  # Filtre les documents qui ont le champ 'more_info.LastName' défini
    {"$unwind": "$more_info"},
    {"$unwind": "$oa04_affiliations"},  # Décompacte les documents pour accéder à chaque auteur individuellement
    {"$group": {
        "_id": {
            "LastName": "$more_info.LastName",
            "ForeName": "$more_info.ForeName"
        },
        "AffiliationTypes": {"$addToSet": "$oa04_affiliations.AffiliationType"}  # Utilise $addToSet pour éviter les doublons
    }},
    {"$match": {"AffiliationTypes.1": {"$exists": True}}}, #{'count': 48183} (sans:{'count': 81734})
    {"$count": "count"}
]

docs_switched = collection.aggregate(pipeline)
authors_switched = [entry for entry in docs_switched]
print(authors_switched)

# 6) Find affiliation with the word "China" 
aff_china = collection.find({
    "oa04_affiliations": {"$exists": True},  
    "oa04_affiliations.Affiliation": {"$regex": "China", "$options": "i"}}, 
    {"_id": 1, "oa04_affiliations.Affiliation":1}).limit(100)

documents = [entry for entry in aff_china]
print(documents[0:9])

# 7) Get the pmids of papers published in 2019
doc = collection.find({'more_info.PubYear':2019},{"_id": 1, 'pmid_list':1})
documents = [entry for entry in doc]
documents

# 8) Count the number of doc with "oa06_researcher_education" OR "oa04_affiliations" key and with the "oa06_researcher_education" AND "oa04_affiliations" .





