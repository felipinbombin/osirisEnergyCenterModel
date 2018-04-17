# -*- coding: utf-8 -*-
# Ejemplo que sirve para linéa de tracción DC independiente
import psycopg2

import RedLineas
import LeerDatos
import GuardarDatos

from psycopg2.extras import DictCursor

db_name = "centroenergia"
host = "localhost"
db_user = 'osiris'
db_pass = 'osiris'
port = 5432

db_connection = psycopg2.connect(host=host, port=5432, user=db_user, password=db_pass, dbname=db_name, cursor_factory=DictCursor)

# Crear diccionario con datos y escenario de simulación de la línea a partir de la base de datos
DatosLinea1 = LeerDatos.DatosDC('Linea1', '2017-01-01 00:00:00', '2017-01-01 23:59:00', db_connection)

# Crear objeto linea
Linea1 = RedLineas.Linea('Linea1', DatosLinea1)

# Definir escenario de simulación con el diccionario creado anteriormente
Linea1.DefinirSimulacion(DatosLinea1)

# Simular operación conjunto de red AC y línea de metro con potencia base Sbase = 1 [MW]
Linea1.simular()

# Guardar resultados de simulaciones de líneas en base de datos
GuardarDatos.saveDCresults(Linea1.ID, Linea1.saveresults(), 'sim1', db_connection)

# Graficar resultados para red de tracción DC de línea
Linea1.plotresults()