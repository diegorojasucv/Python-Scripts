import pandas as pd
import numpy as np

# Dataframe ejemplo
data = {"Team": ["Red Sox", "Red Sox", "Red Sox", "Red Sox", "Red Sox", "Red Sox", "Yankees", "Yankees", "Yankees", "Yankees", "Yankees", "Yankees"],
		"Pos": ["Pitcher", "Pitcher", "Pitcher", "Not Pitcher", "Not Pitcher", "Not Pitcher", "Pitcher", "Pitcher", "Pitcher", "Not Pitcher", "Not Pitcher", "Not Pitcher"],
		"Age": [24, 28, 40, 22, 29, 33, 31, 26, 21, 36, 25, 31]}

# Funcion GROUP BY en SQL
def group_by(df, var_groupby, agregation):
    ''' Función que replica el GROUP_BY de SQL'''

    df_grouped = df.groupby(var_groupby).agg(agregation).reset_index()
    
    # Unimos los nombres de los keys y values del diccionario agregation
    new_columns=[]
    for i in range(len([i for i in agregation.values()][0])):
        for k,v in agregation.items():
            new_columns.append(k+'_'+v[i])
    
    # Eliminamos el nivel 1 y sustituimos el nombre de las columnas
    df_grouped.columns = df_grouped.columns.droplevel(1)
    df_grouped.columns = var_groupby + new_columns
    
    return df_grouped

# Parametros
var_groupby = ['Team', 'Position']
agregation = {'Age':['count', 'sum', 'min']}

df_new = group_by(df, var_groupby, agregation)
df_new

# Función para crear una Tabla Dinamica
def tabla_dinamica(df, indices, columnas, valor, funcion_agrupamiento):
    df_new = pd.pivot_table(df,index=indices, columns=columnas, 
                            values=valor, aggfunc=funcion_agrupamiento, fill_value=0)
    return df_new

# Parametros
indices = ['Pos']
columnas = ["Team"]
valor = ["Age"]
funcion_agrupamiento = {"Age":[len,np.sum,np.mean]}

tabla_dinamica(df, indices, columnas, valor, funcion_agrupamiento)

# Funcion ROW_NUMBER en SQL
def row_number(df, partitionby, orderby, asc_desc, filtrado, row = 1):
    ''' Función que replica el ROW_NUMBER de SQL'''

    if filtrado==0:
        df["row_number"] = df.groupby(partitionby)[orderby].rank(method = "first", ascending = asc_desc)
        df = df.sort_values(partitionby)
    elif filtrado==1:
        df["row_number"] = df.groupby(partitionby)[orderby].rank(method = "first", ascending = asc_desc)
        df = df.sort_values(partitionby)
        df = df[df["row_number"] == row]
    return df

# Parametros    
partitionby = ['Team','Pos']
orderby = ['Age']
asc_desc = 0 #1: ascendente, 0: descendente
filtrado = 0 #1: filtra el output según rank, 0: No filtra el output

df_new = row_number(df, partitionby, orderby, asc_desc, filtrado)
df_new

# Funcion PIVOT en SQL
def pivot(df, variables, value):
    ''' Función que replica PIVOT de SQL'''

    df_new = df.melt(id_vars = variables, value_vars = value, 
                     var_name = '_'.join(value) + '_changed', value_name = '_'.join(value) + '_value')
    return df_new

# Parametros  
variables = ['Team','Age']
value = ['Position']

df_new = pivot(df, variables, value)
df_new