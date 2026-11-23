import os
from pymongo import MongoClient

_c = None

def bd():
    global _c
    if _c is None:
        _c = MongoClient(os.environ['MONGO_URI'], w='majority', readConcernLevel='majority', serverSelectionTimeoutMS=4000)
    return _c.get_database('recursos_borde')

def asegurar_indices(d):
    d.mensajes_procesados.create_index('message_id', unique=True)
    d.sagas.create_index('saga_id', unique=True)
    d.reservas.create_index('saga_id', unique=True)
    d.eventos_log.create_index('seq')
