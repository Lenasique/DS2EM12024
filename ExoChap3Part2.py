import json 
import zipfile
import pymongo
import datetime

client = pymongo.MongoClient('localhost', 27017)
mydb = client["Chap3_real"]
collection = mydb["application1"]

# Ouvrir le fichier ZIP en mode lecture
with zipfile.ZipFile('data/Chap3/pubmed_cleaned.zip', 'r') as zip_ref:
    # Extraire le fichier JSON
    zip_ref.extract('pubmed_cleaned.json')

# Ouvrir le fichier JSON et le charger en tant que structure de données Python
with open('pubmed_cleaned.json', 'r') as file:
    data = json.load(file)
    file.close()

for i in range(len(data)):
    try:
        data[i]['_id'] = data[i]['_id'].pop('$oid')
    except Exception as e:
        print(e, i)

for entry in data:
    try:
        entry['authors'] = entry['authors'].split('\n')
    except Exception as e:
        print(e, entry)

#_li = []
#for entry in data[2]['authors'].split('\n'):   #.replace('\n',',').split(',')
#    name = entry.split(',')[0]
#    affiliation = ''.join(entry.split(',')[1:])
#    _li.append([name, affiliation])
#print(_li)
     
for entry in ['date', 'date_medline', 'date_received', 'date_accepted']:
    for i in range(len(data)):
        try:
            _list = data[i][entry].replace(',', '').split(' ')
            data[i][entry] = datetime.datetime(int(_list[1]), int(_list[3]), int(_list[5]))
        except Exception as e:
            print(e, i)
            
collection.insert_many(data)

# 1) Create an index, explain your choice of key
collection.create_index([("_id", 1)])

# 2) Delete every paper that was published prior 2019
collection.delete_many({'date': {"$lt": datetime.datetime(2019, 1, 1)}})

# 3) How many papers have a single author? Two authors?
collection.count_documents({"authors": {"$size": 1}})
collection.count_documents({"authors": {"$size": 2}})

# 4) What's the last paper inserted in the db?
collection.find_one(sort=[("date", pymongo.DESCENDING)])['title']

# 5) Find articles with null meshwords
articles = [entry for entry in collection.find({"meshwords": None})]
articles[0:4]

# 6) Choose a keyword you are interested in (machine learning, computer vision, etc.).
#    Find the number of articles with the chosen keyword in their meshwords, abstract, or title
keyword = "machine learning"
collection.count_documents({
    "$or": [
        {"meshwords": keyword},
        {"abstract": {"$regex": keyword, "$options": "i"}},
        {"title": {"$regex": keyword, "$options": "i"}}
    ]
})

# 7) What's the number of articles that have at least one affiliation AND meshwords?
collection.count_documents({
    "$and": [{"source": {"$exists": True}},
             {"source": {"$ne": None}},            #il n'y a pas de key 'affiliation' 
             {"meshwords": {"$exists": True}},    #################################""
             {"meshwords": {"$ne": None}}]##########################################
})

collection.count_documents({'meshwords': {"$exists": True}})

# 8) How many articles have a publishing date after 2020?
collection.count_documents({"date_medline": {"$lt": datetime.datetime(2020,1,1)}})

# 9) Find articles where there's at least one affiliation from a chosen country (you decide which one).
country = "USA"
collection.find({"affil": {"$regex": country}})   ###############################################

# 10) Check for any duplicates. (Hint: look at the DOI or the PMID)
duplicate_articles = collection.aggregate([
    {"$group": {"_id": {"doi": "$doi", "pmid": "$pmid"}, "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}}
])

# 11) Remove every article where the abstract starts with an "R".
collection.delete_many({"abstract": {"$regex": "^R"}})

# 12) Return the list of papers (PMID) where there's at least one affiliation per author.
papers_with_one_affiliation_per_author = collection.find({"$expr": {"$eq": [{"$size": "$affiliation"}, {"$size": "$authors"}]}})

