# Challenge Alkemy Data Analytics - Python

## API
_Se utilizo la API [CKAN](https://datos.gob.ar/acerca/ckan) para acceder a la información y los datos desde la pagina del gobierno_

## Deploy
_Primero se deberá insalar las dependencias._
```
$ pip install -r requirements.txt
```
> Nota: Primero crear un entorno virtual
>
> ```
> $ virtualenv etl_venv -p python3
> $ source etl_venv/bin/Script/activate
> $ pip install -r requirements.txt
> ```

_Luego deberá correr el script del pipeline._
```
$ python pipeline.py
```
```
Pipeline creado
      extrayendo la información desde la API...
      formateando y procesando la información...
      cargando los resultados a la base de datos...

Proceso finalizado correctamente.
```
## Conexion a la base de datos
_Para realizar la conexión a la base de datos PostgreSQL deberá configurar sus credenciales en un archivo .env, puede guiarse del archivo "example_env"._

_Para acceder a la base de datos, use el ORM SQLalchemy y el dialecto psycopg2._
















>  Nota: Esta no es la forma final, ni la mejor forma de afrontar el challenge, solo puse en practica mis conocimientos y desconocimientos, aprendiendo mucho cada día.
