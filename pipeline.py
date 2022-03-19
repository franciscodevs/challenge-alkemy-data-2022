'''LIBRERIAS '''
import os
import time
import json
import locale
import requests
import psycopg2
import pandas as pd
from decouple import config
from sqlalchemy import text
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

locale.setlocale(locale.LC_ALL, ("es_ES", "UTF-8"))

################################TENGO QUE USAR EXCEPCIONES EN CADA PASO DEL ETL
''' CONEXION A LA API '''
print("extrayendo la información desde la API...")
try: #Probar si la API funciona correctamente
    package_information_url = 'http://datos.gob.ar/api/3/action/package_show?id=cultura-mapa-cultural-espacios-culturales'
    package_information = requests.get(package_information_url)
    package_dict = json.loads(package_information.content)
    package_dict = package_dict['result']
except:
    print("Error, no se pudo conectar a la API")

dataset = []
for dic in package_dict['resources']:
    interest_list =  ['Museo', 'Salas de Cine', 'Bibliotecas Populares']
    if dic['name'] in interest_list:
        dataset.append(dic['url'])

biblioteca_popular = dataset[0]
museos = dataset[1]
salas_de_cine = dataset[2]


''' EXTRACCION DE LOS DATASET '''
def download_file(dataset, filename):
    #pido el archivo desde la url
    r = requests.get(dataset)
    #formateo el nombre
    nombreArchivo = filename
    nombre = nombreArchivo + time.strftime("-%d-%m-%Y") + ".csv"
    directorio = config("DATA_FOLDER_DIRECTORY") # directorio en donde iran los datos
    #creo las carpetas necesarias
    folder_name = filename
    anio_mes = datetime.now().strftime("%Y-%B")
    first_folder = os.path.join(directorio, folder_name) #primera carpeta
    second_folder = os.path.join(directorio  + folder_name, anio_mes) #segunda carpeta
    file_name = nombre
    file = os.path.join(second_folder, file_name)
    os.makedirs(second_folder)
    #leo y guardo el archivo en un formato .csv
    with open(file, 'wb') as f: #"wb" para abrirlo en modo binario (el mas acertado para un .csv)
        for chunk in r:
            f.write(chunk)

download_file(biblioteca_popular, "biblioteca_popular")
download_file(museos, "museos")
download_file(salas_de_cine, "salas_de_cine")

'''TRANSFORMACION DE LOS DATOS'''
print("formateando y procesando la información...")

directorio = config('DATA_FOLDER_DIRECTORY')
#Busco el directorio en donde se encuentra almacenado el .csv
def find_csv_dir(name):
    today = time.strftime("-%d-%m-%Y")
    for (dirpath, dirnames, filenames) in os.walk(directorio):
        for filename in filenames:
            if filename.endswith(name + today + ".csv"):#Aqui obtengo el archivo .csv con la fecha actual
                file = (dirpath + '\\' + filename ).replace('\\', '/')
                return(file)

biblioteca_popular_path = find_csv_dir("biblioteca_popular")
museo_path = find_csv_dir("museos")
cine_path = find_csv_dir("salas_de_cine")

'''Procesamiento de datos '''

#Defino los dataframes
df_biblio = pd.read_csv(biblioteca_popular_path)
df_museo = pd.read_csv(museo_path)
df_cine = pd.read_csv(cine_path)

#Renombro las columnas para la normalizacion
df_biblio = df_biblio.rename(columns={'Cod_Loc':'cod_localidad',
                   'IdProvincia':'id_provincia',
                   'IdDepartamento':'id_departamento',
                   'Categoría':'categoría',
                   'Provincia':'provincia',
                   'Localidad':'localidad',
                   'Nombre':'nombre',
                   'Domicilio':'domicilio',
                   'CP':'código_postal',
                   'Teléfono':'número_de_teléfono',
                   'Mail':'mail',
                   'Web':'web',
                    'Fuente':'fuente'})
df_museo = df_museo.rename(columns={'cod_loc':'cod_localidad',
                   'idprovincia':'id_provincia',
                   'iddepartamento':'id_departamento',
                   'categoria':'categoría',
                   'direccion':'domicilio',
                   'CP':'código_postal',
                   'telefono':'número_de_teléfono'})
df_cine = df_cine.rename(columns={'Cod_Loc':'cod_localidad',
                   'IdProvincia':'id_provincia',
                   'IdDepartamento':'id_departamento',
                   'Categoría':'categoría',
                   'Provincia':'provincia',
                   'Localidad':'localidad',
                   'Nombre':'nombre',
                   'Dirección':'domicilio',
                   'CP':'código_postal',
                   'Teléfono':'número_de_teléfono',
                   'Mail':'mail',
                   'Web':'web',
                   'Fuente':'fuente',
                   'Pantallas':'pantallas',
                   'Butacas':'butacas',
                   'espacio_INCAA':'espacio_incaa'})
#Uno los datos conjuntos
frames = [df_cine, df_biblio, df_museo]
result = pd.concat(frames)

#Proceso los datos Conjuntos

'''Tabla normalizada'''
df_normalizado = result[['cod_localidad','id_provincia','id_departamento','categoría',
                            'provincia','localidad','nombre','domicilio','código_postal',
                            'número_de_teléfono','mail','web']]

'''Cantidad de registros'''
registros = pd.DataFrame(result.groupby(['provincia','categoría','fuente']).agg('cod_localidad').size()).reset_index()
df_registros = registros.rename(columns={'cod_localidad':'registros_totales'})

'''Cine Procesado'''
df_cine_procesado = df_cine[['provincia','pantallas','butacas','espacio_incaa']].copy()
df_cine_procesado['espacio_incaa'].replace({'0': 0, 'si' : 1, "SI": 1}, inplace=True)


'''CONEXION A LA BASE DE DATOS'''
print("cargando los resultados a la base de datos...")
#Credenciales de PostgreSQL
host = config("HOST")
user = config("USER")
password = config("PASSWORD")
database = config("DATABASE")
port = config("PORT")

url =  f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
engine = create_engine(url)
connection = engine.connect()

#Creacion de toda la informacion de Museos, Salas de Cine y Bibliotecas (datos_ar)
if not engine.dialect.has_table(connection, "datos_ar"):
    file = open("src/sql/datos_ar.sql", encoding='utf-8')
    query = text(file.read())
    connection.execute(query)
else:
    pass

#Creacion del total de los datos conjuntos con Categoria, Fuente y Provincia (total_registros)
if not engine.dialect.has_table(connection, "total_registros"):
    file = open("src/sql/total_registros.sql", encoding='utf-8')
    query = text(file.read())
    connection.execute(query)
else:
    pass

#Creacion de la informacion de Cines que contiene Provincia, cantidad de butacas, pantallas y espacios INCAA(info_cines)
if not engine.dialect.has_table(connection, "info_cines"):
    file = open("src/sql/info_cines.sql", encoding='utf-8')
    query = text(file.read())
    connection.execute(query)
else:
    pass
'''Cargamos los dataframes a la base de datos PostgreSQL'''
df_registros.to_sql('total_registros', engine, if_exists='append', index=False)

df_normalizado.to_sql('datos_ar', engine, if_exists='append', index=False)

df_cine_procesado.to_sql('info_cines', engine, if_exists='append', index=False)

###
print("Proceso finalizado correctamente.")
