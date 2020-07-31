"""
Objetivo: Funciones para Manipulación de Datos
Elaborado por: Diego Rojas

"""
import pandas as pd
import numpy as np

# Dataframe ejemplo
data = {"Team": ["Red Sox", "Red Sox", "Red Sox", "Red Sox", "Red Sox", "Red Sox", "Yankees", "Yankees", "Yankees", "Yankees", "Yankees", "Yankees"],
		"Pos": ["Pitcher", "Pitcher", "Pitcher", "Not Pitcher", "Not Pitcher", "Not Pitcher", "Pitcher", "Pitcher", "Pitcher", "Not Pitcher", "Not Pitcher", "Not Pitcher"],
		"Age": [24, 28, 40, 22, 29, 33, 31, 26, 21, 36, 25, 31]}

#################### Funcion GROUP BY en SQL ####################
def group_by(df, var_groupby, agregation):
    ''' Función que replica el GROUP_BY de SQL'''

    df_grouped = df.groupby(var_groupby).agg(agregation).reset_index()
    
    # Unimos los nombres de los keys y values del diccionario agregation
    new_columns=[]
    for i in range(len([i for i in agregation.values()][0])):
        for k, v in agregation.items():
            new_columns.append(k + '_' + v[i])
    
    # Eliminamos el nivel 1 y sustituimos el nombre de las columnas
    df_grouped.columns = df_grouped.columns.droplevel(1)
    df_grouped.columns = var_groupby + new_columns
    
    return df_grouped

# Parametros
var_groupby = ['Team', 'Position']
agregation = {'Age':['count', 'sum', 'min']}

df_new = group_by(df, var_groupby, agregation)
df_new

#################### Función para crear una Tabla Dinamica ####################
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

#################### Funcion ROW_NUMBER en SQL ####################
def row_number(df, partitionby, orderby, asc_desc, filtrado, row = 1):
    ''' Función que replica el ROW_NUMBER de SQL'''

    if filtrado=='todos':
        df["row_number"] = df.groupby(partitionby)[orderby].rank(method = "first", ascending = asc_desc)
        df = df.sort_values(partitionby)
    elif filtrado=='igual a':
        df["row_number"] = df.groupby(partitionby)[orderby].rank(method = "first", ascending = asc_desc)
        df = df.sort_values(partitionby)
        df = df[df["row_number"] == row]
    elif filtrado=='menor a':
        df["row_number"] = df.groupby(partitionby)[orderby].rank(method = "first", ascending = asc_desc)
        df = df.sort_values(partitionby)
        df = df[df["row_number"] <= row]
    elif filtrado=='mayor a':
        df["row_number"] = df.groupby(partitionby)[orderby].rank(method = "first", ascending = asc_desc)
        df = df.sort_values(partitionby)
        df = df[df["row_number"] >= row]
    return df

# Parametros    
partitionby = ['Team','Pos']
orderby = ['Age']
asc_desc = 0 #1: ascendente, 0: descendente
'''
    todos: trae todas las filas sin filtrar.
    igual a: filtra las filas según el row definido.
    menor a: filtras todas las filas menores al row definido.
    mayor a: filtras todas las filas mayores al row definido.
'''
filtrado = 'menor a' 
row = 2 # definido 1 por defecto

df_new = row_number(df, partitionby, orderby, asc_desc, filtrado)
df_new

#################### Funcion PIVOT en SQL ####################
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

# Dataframe Ejemplo
df = pd.DataFrame({'Name':['Jack','Sue','Robert','Bob','Alice','John'],
    'A': [1, 2.1, pd.np.nan, 4.7, 5.6, 6.8],
    'B': [.25, pd.np.nan, pd.np.nan, 4, 12.2, 14.4],
    'City':['SF','SF','LA','OC','LA','OC']})

#################### Situación de los datos ####################

# Situación de los datos
def status(df):
    resumen = pd.DataFrame()
    resumen['no_casos'] = df.count()    
    resumen['no_nulos'] = df.isnull().sum()
    resumen['porc_nulos'] = resumen.no_nulos/resumen.no_casos
    resumen['no_ceros'] = df.apply(lambda x: x[x == 0].count())
    resumen['porc_ceros'] = resumen.no_ceros/resumen.no_casos
    resumen['no_unicos'] = df.apply(lambda x: x.drop_duplicates().count())
    resumen['porc_unicos'] = resumen.no_unicos/resumen.no_casos
    resumen['tipo_dato'] = df.dtypes
    return resumen

status(df)

#################### Imputación de valores nulos para las variables númericas ####################

# 1) Usando apply
df.apply(lambda x: x.fillna(x.mean()) 
            if x.dtype.kind in 'biufc' 
            else x.fillna('N/A')
        )

# 2) Usando for
for x in df.select_dtypes([np.number]).columns:
    df[x] = df[x].fillna(df[x].mean())


# 3) Usando sklearn
def imputar_nulos(df, estrategia):
    from sklearn.impute import SimpleImputer
    imputer = SimpleImputer(missing_values = np.nan, strategy = estrategia)
    imputer = imputer.fit(df.select_dtypes([np.number]))
    df.loc[:, df.select_dtypes([np.number]).columns] = imputer.transform(df.select_dtypes([np.number]))
    return df

df = imputar_nulos(df, estrategia = 'mean')

#################### Codificar variables categoricas ####################

# 1) Usando sklearn
def codificar_categoricas(df, todas = 1):
    from sklearn import preprocessing
    if todas==1:
        le_X = preprocessing.LabelEncoder()
        index = [df.columns.get_loc(c) for c in df.select_dtypes(object).columns]
        for i in index:
            df.iloc[:,i] = le_X.fit_transform(df.iloc[:,i])
    elif todas==0:
        le_X = preprocessing.LabelEncoder()
        var = input("Introduzca el/los nombres de la variables a codificar separadas por espacio:")
        var = var.split()
        for i in var: 
            df.loc[:,i]= le_X.fit_transform(df.loc[:,i])
    return df

#df = codificar_categoricas(df, 0)
#df

#################### Crear variables Dummy ####################

# 1) Usando sklearn
def one_hot_encoder(df, formato_categoricas = 'object'):
    from sklearn.preprocessing import OneHotEncoder
    ohe = OneHotEncoder()
    df_object = df.select_dtypes(formato_categoricas)
    ohe.fit(df_object)
    codes = ohe.transform(df_object).toarray()
    feature_names = ohe.get_feature_names()
    df = pd.concat([df.select_dtypes(exclude = formato_categoricas), 
                   pd.DataFrame(codes,columns = feature_names).astype(int)], axis=1)
    return df
    
#df = one_hot_encoder(df, 'object')

# 2) Usando pandas
#pd.get_dummies(df)

#################### Case When ####################

# 1) Para datos categoricos

# Usando if-else
def clasificar(variable):
    if variable == 'Red Sox':
        return 'Team_1'
    elif variable == 'Yankees':
        return 'Team_2'

df['Team'] = df.Team.apply(clasificar)

# Usando map
dict ={'Red Sox': 'Team_1',
      'Yankees': 'Team_2' 
      }
df['Team'] = df.Team.map(dict)

# Usando replace
df.replace(
    {'Team': {'Red Sox': 'Team_1',
               'Yankees': 'Team_2' 
            }},
    inplace = True
)

# Usando get
def case_when(data):
        switcher={
                'Red Sox': 'Team_1',
               'Yankees': 'Team_2' 
                 }
        func = switcher.get(data, 'si')
        return func 
    
df['Team'] = df.Team.apply(case_when)

# 2) Para datos numericos

# Usando mask
df['Age_Category'] = df.Age.mask(df.Age <= 25, 'Child').mask(df.Age > 25, 'Adult')

# Usando cut
bins = [0,4,25,40,99]
labels =['Toddler','Child','Adult','Elderly']

df['Age_Category'] = pd.cut(df['Age'], bins = bins, labels = labels)

#################### Otros ####################

# Para ocultar indices
df.head().hide_index()

# Para cocultar variables
df.head().hide_columns()