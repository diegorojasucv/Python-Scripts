from pymongo import MongoClient
from pymongo import ASCENDING,DESCENDING

# Ver las bases de datos que tengo
client = MongoClient('localhost')
print(client.list_database_names())

# Crear una base de datos
db=client['prueba']
# Crear una coleccion
col=db['personas']
# Agregar documento a colección
col.insert_one({
    'edad':20,
    'nombre':'Diego',
    'intereses':['Musica','Youtube']
})

# Ver cantidad de documentos en la colección
print(db.list_collection_names())
print(col.count_documents({}))

# Insertar varios documentos
col.insert_many([
    {'edad':24,
    'nombre':'Ricardo',
    'intereses':['Netflix','Youtube']},
    {'edad':25,
    'nombre':'Andres',
    'intereses':['Spotify','Youtube']},
    {'edad':30,
    'nombre':'Alejandro',
    'intereses':['Musica','Baseball']}
])

# Recorrer todos documentos
for documentos in col.find({}):
    print(list(documentos))

# Query para obtener todos documentos con proyeccion de algunas columnas:nombre e intereses
for documentos in col.find(
                           {'edad':{'$gt':24}},
                           { 'nombre': 1, 'intereses': 1, '_id': 0 }):
print(documentos)

# Eliminar documentos donde la edad sea 20
col.delete_many({
    'edad':20
})

for documentos in col.find({}):
    print(documentos)

# Hacer update de un documento
col.update_one({
    'edad':24
},
    {'$set':{'edad':27}
})

for documentos in col.find({}):
    print(documentos)

# Crear un indece
col.create_index([('edad',ASCENDING)])

# Borrar una coleccion
db.drop_collection('personas')
print(db.list_collection_names())

# Eliminar la base de datos
client.drop_database('prueba')
print(client.list_database_names())

#Cerrar el cliente
client.close()