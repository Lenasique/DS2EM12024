import pymongo
import tqdm
import random
from collections import *
from lxml import etree
import requests
import re
import names
import pickle
import time 
import json
from PIL import Image
from matplotlib import pyplot
import numpy as np
from bson.binary import Binary
import string

#**1**: Take the dict created in the TODO 4 in chapter I and save it in the collection "CRUD_exercise".
client = pymongo.MongoClient('localhost', 27017)
mydb = client["Todo"]
collection = mydb["CRUD_exercice"]

with open('data/Chap2/papers.json', 'r') as file:
    json_papers = json.load(file)
    file.close()

collection.insert_many(json_papers)

#**2**: Insert 3 documents with key = x and values = 1, delete one of them. Which one is deleted first ? the most recent or oldest one ? increment the value of x to 4.
for i in tqdm.tqdm(range(3)):
    post = {"x":1}
    collection.insert_one(post)

collection.delete_one({'x': 1}) #Le premier est supprimé (le plus vieux)
collection.update_many({'x': 1}, {'$inc': {'x': 3}}) #incrément de 3

#**3**: Insert the dict created in the TODO 6 Chapter I in the example collection.
mydb2 = client['tutorial']
collection2 = mydb2['example'] #creation de la collection 'example'

with open('data/Chap2/json_file2.json', 'r') as file:
    json_papers = json.load(file)
    file.close()

print(json_papers)
collection2.insert_one(json_papers)

#**4**: Get documents where authors key exist in the collection "CRUD_exercise".
documents = collection.find({"authors": {"$exists": True}})
[print(doc) for doc in documents]

#**5**: Change the documents where x = 4 to x = 1.
collection.update_many({'x': 4}, {'$inc': {'x': -3}})

#**6**: Find documents where author is not_mike and set author as real_mike.
documents = collection2.find_one_and_update({'author': "not_mike"}, {'$set': {'author': "real_mike"}})
print(documents)

#**7**: Delete documents where author is real_mike.
collection2.delete_many({'author': 'real_mike'})

#**8**: create a collection named "CRUD_exercise_benchmark" with 500k observations, ids increment of 2 (sequence:0,2,4,6,...1M). Give a random np.array with a key named "values" and use the insert_many. Then create an index on the id and benchmark queries before and after indexing. Did the index help ?
db = client['Todo']
collection3 = db['CRUD_exercise_benchmark']

#Création de la collection et test sans index
documents = [{'_id': i, 'values': np.random.rand(10).tolist()} for i in range(0, 1000000, 2)]
collection3.insert_many(documents)

t = time.time()
collection3.find({'_id': {'$gt': 499000}})
t1 = time.time() - t 

#Création de l'index et test avec l'index
collection3.create_index([('doc_id', 1)])

t = time.time()
collection3.find({'doc_id': {'$gt': 499000}})
t2 = time.time() - t

print(f'Time before indexing: {t1}' + '\n' + f'Time after indexing: {t2}')
#Baisse du temps de recherche

#**9**: create a random collection in a random db and put the new collection in the tutorial DB
def random_string(length=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

#Création de la database et de la collection
db_name = random_string()
collection_name = random_string()
random_db = client[db_name]
random_collection = random_db[collection_name]

documents = [{'name': random_string(5), 'value': random.randint(1, 100)} for _ in range(50)]
random_collection.insert_many(documents)

#Transfert dans la db tutorial
collection3 = mydb2[collection_name]
data = list(random_collection.find({}))
collection3.insert_many(data)

client.drop_database(db_name) #drop la plus ancienne

#**10**: What is the difference between an inner join and an outer join ? Is the query seen during course an inner or outer join ? Play with the query to show all the joins.
def mongo_join(left:str, right:str, type:str, id, keep:dict=None): 
    '''
    Fuction to create pipeline to join two collections

    left: left collection
    right: right collection 
    id: join on id
    type: 'left', 'right', 'inner', 'full'
    keep: key to keep using pymongo $project
    '''
    pipeline = [{'$lookup': 
               {'from' : right,
                'localField' : id,
                'foreignField' : id,
                'as' : 'cellmodels'}},
        {'$unwind': '$cellmodels'}]
    
    if type == 'right':
        pipeline['from'] = left
        
    elif type == 'left':
        pipeline.append({
            '$unwind': {
                'path': '$cellmodels',
                'preserveNullAndEmptyArrays': True
            }
        })

    elif type == 'full':
        pipeline.append({
            '$unwind': {
                'path': '$cellmodels',
                'preserveNullAndEmptyArrays': True
            }
        })

    elif type == 'inner':
        pipeline.append({'$unwind': '$cellmodels'})

    if keep : pipeline.append({'$project': keep}) 

    return print(pipeline)

documents = collection.aggregate()

#**11**:  Use the oaipmh and api code get papers after January 2020 and for "cs,math,econ" categories. Insert them in MongoDB. Import only the first 200. How is it sorted ? How can you define your own sort()? Query papers to get papers after 2021, which have 3 authors and with domain "cs".
def xml_to_dict(tree, paths=None, nsmap=None, strip_ns=False):
    """Convert an XML tree to a dictionary.
    :param tree: etree Element
    :type tree: :class:`lxml.etree._Element`
    :param paths: An optional list of XPath expressions applied on the XML tree.
    :type paths: list[basestring]
    :param nsmap: An optional prefix-namespace mapping for conciser spec of paths.
    :type nsmap: dict
    :param strip_ns: Flag for whether to remove the namespaces from the tags.
    :type strip_ns: bool
    """
    # if xpath empty take every path (.//)
    paths = paths or ['.//']
    nsmap = nsmap or {}
    # defaultdict = never return a keyerror but an empty list
    fields = defaultdict(list)
    for path in paths:
        elements = tree.findall(path, nsmap)
        for element in elements:
            tag = re.sub(
                r'\{.*\}', '', element.tag) if strip_ns else element.tag
            fields[tag].append(element.text)
    return dict(fields)

sets = ['cs', 'math', 'econ']
XMLParser = etree.XMLParser(remove_blank_text=True, recover=True, resolve_entities=False)

# Initialiser une liste pour stocker les 200 premiers articles
papers_to_insert = []

for set_ in tqdm.tqdm(sets):
    response = requests.get(f"http://export.arxiv.org/oai2?verb=ListIdentifiers&set={set_}&metadataPrefix=oai_dc")
    tree = etree.XML(response.content, parser=XMLParser)
    papers = xml_to_dict(tree=tree)
    
    # Itérer sur les en-têtes et ajouter ceux après janvier 2020 à la liste
    for header in tree.xpath('//oai:ListIdentifiers/oai:header', namespaces={'oai': 'http://www.openarchives.org/OAI/2.0/'}):
        datestamp = header.find('oai:datestamp', namespaces={'oai': 'http://www.openarchives.org/OAI/2.0/'}).text
        if int(datestamp[:4]) > 2020:
            papers_to_insert.append(header)

# Insérer les 200 premiers articles dans la collection MongoDB
collection.insert_many(papers_to_insert[:200])

# Comment sont-ils triés ? MongoDB les trie par ordre d'insertion par défaut.
# Vous pouvez définir votre propre tri en utilisant la méthode sort() lors de la récupération des données.

# Interroger les articles pour obtenir ceux après 2021, avec 3 auteurs et dans le domaine "cs"
query_result = collection.find({
    'datestamp': {'$gt': '2021-01-01'},
    'authors': {'$size': 3},
    'category': 'cs'
})

for paper in query_result:
    print(paper)


#**12**: Do the same as exercise 8 but with the connection to the cluster. Then check the metrics and take screenshot of opcounters, logical size and connections.

#**13**: Download a random image and store it in a collection.

#**14**: Try to store a pandas dataframe in mongoDB (array with rownames, array with colnames and matrix with values)

#**15**: Insert the movie_review.tsv data into mongodb. Then query it to find the number of review that are positive and negative review. Fetch the docs which have "unexpected" in their review, how many are they ? Think of a clever way to count the number of words in the review using MongoDB (hint: Transform the review text before the insert in MongoDB) and create a density of number of words per review.

#**16**: Download a [sound sample](https://freesound.org/browse/). Try to store it in MongoDB 

#**17**: Create a collection with 30M observation with a single key : "year" which is a random value between 2000-2020. Get documents with year = 2000. Does using an index helps ? 







