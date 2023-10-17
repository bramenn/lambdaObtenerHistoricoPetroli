import os
from pymongo import MongoClient, DESCENDING
from typing import Dict
from json import loads
from bson.json_util import dumps

client = MongoClient(host=os.environ.get("MONGO_URI"))


def lambda_handler(event, context):

    numero_registros = event.get("numero_datos", 100)
    query = event.get("query")
    

    db = client.petrolera

    collection = db.eventos_activos_petroleros


    result = collection.find(query).limit(int(numero_registros)).sort([("timestamp", DESCENDING)])
    
        
    return loads(dumps(result))
