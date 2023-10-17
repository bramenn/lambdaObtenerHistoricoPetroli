import os
from pymongo import MongoClient
from typing import Dict
from json import loads

client = MongoClient(host=os.environ.get("MONGO_URI"))


def lambda_handler(event, context):
    
    if not event.get("Records"):
        return "No existen eventos"
        
    if not event.get("Records")[0].get("Sns"):
        return "No se contraron eventos de colas SNS"
    
    if event.get("Records")[0].get("Sns").get("Type") != "Notification":
        return "No es un evento de notificación"
        
    if not event.get("Records")[0].get("Sns").get("Message"):
        return "Ningun mensaje encontrado"
    
    
    
    data: Dict = loads(event.get("Records")[0].get("Sns").get("Message"))


    db = client.petrolera

    collection = db.eventos_activos_petroleros

    result = collection.insert_one(data)

    if result.inserted_id:
        return "Document inserted successfully"
    else:
        return "Failed to insert document"