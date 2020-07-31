import pandas as pd
import sqlite3

# Creamos la conexion
conn = sqlite3.connect('orders.db')

# El cursor nos permite ejecutar queries en la base de datos
cur = conn.cursor()

#con la siguiente sintaxis podemos ejecutar queries. Tenemos que utilizar comillas dobles, simples o triples.
#cur.execute("YOUR-SQL-QUERY-HERE;")

# Crear tablas
cur.execute("""CREATE TABLE IF NOT EXISTS users(
   userid INT PRIMARY KEY,
   fname TEXT,
   lname TEXT,
   gender TEXT);
""")

# Confirmamos los cambios usando la función commit en el objeto de conexión.
conn.commit()

# Agregar registros a una tabla
users=[('00001', 'Nik', 'Piepenbreier', 'male'),
       ('00002', 'Lois', 'Lane', 'Female'),
       ('00003', 'Peter', 'Parker', 'Male'), 
       ('00004', 'Bruce', 'Wayne', 'male')]

cur.executemany("INSERT INTO users VALUES(?, ?, ?, ?);", users)
conn.commit()

#Seleccionando algunos registros

# fetchone()
cur.execute("SELECT * FROM users;")
one_result = cur.fetchone()
print(one_result)

# fetchmany() 
cur.execute("SELECT * FROM users;")
three_results = cur.fetchmany(3)
print(three_results)

# fetchall()
cur.execute("SELECT * FROM users;")
all_results = cur.fetchall()
print(all_results)

# Eliminando registros
cur.execute("DELETE FROM users WHERE lname='Parker';")
conn.commit()

cur.execute("select * from users where lname='Parker'")
print(cur.fetchall())

cur.execute("select * from users")
print(cur.fetchall())

# Cerrar la conexion
conn.close()

# Crear tabla con una función

def crear_tabla():
    
    try:
        conexion = sqlite3.connect('ordersnew.db')
        query_drop_tabla="drop table if exists users"
        query_crear_tabla = """CREATE TABLE IF NOT EXISTS users(
                               userid INT PRIMARY KEY,
                               fname TEXT,
                               lname TEXT,
                               gender TEXT);"""

        users=[('00001', 'Nik', 'Piepenbreier', 'Male'),
           ('00002', 'Lois', 'Lane', 'Female'),
           ('00003', 'Peter', 'Parker', 'Male'), 
           ('00004', 'Bruce', 'Wayne', 'Male')]
        query_insertar_tabla="INSERT INTO users VALUES(?, ?, ?, ?);"

        cursor = conexion.cursor()
        print("Conexion a SQLite exitosa")

        cursor.execute(query_drop_tabla)
        conexion.commit()

        cursor.execute(query_crear_tabla)
        conexion.commit()
        print("Tabla Creada")

        cursor.executemany(query_insertar_tabla, users)
        conexion.commit()
        print("Valores insertados correctamente")

        cursor.close()

    except sqlite3.Error as error:
        print("Ha existido un error en el proceso", error)
    finally:
        if (conexion):
            conexion.close()
            print("La conexion a Sqlite ha sido cerrada")

crear_tabla()

# Pasar de Sqlite a un dataframe
conexion = sqlite3.connect('ordersnew.db')
query="SELECT * FROM users;"
df_user = pd.read_sql_query(query, conexion)
df_user_grouped=df_user.groupby('gender',as_index=True).agg({'userid':['count']})
df_user_grouped=pd.DataFrame(df_user_grouped.values, index=df_user_grouped.index.values, columns=['casos'])
df_user_grouped

# Pasar de un df a sqlite
df_user_grouped.to_sql('users_grouped', con=conexion)
conexion.execute("select * from users_grouped").fetchall()

