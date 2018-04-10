# -*- coding: utf-8 -*-
# Ejemplo que sirve para simular red AC y linéa de tracción DC de forma secuencial
import RedAC
import RedLineas
import LeerDatos
import GuardarDatos

# Crear diccionario con datos y escenario de simulación de la red a partir de la base de datos
DatosCochrane = LeerDatos.DatosAC('Cochrane', '2017-01-01 00:00:00', '2017-01-01 23:59:00', 'localhost', 'modelo_datos_metrosolar','root', '1234')

# Crear diccionario con datos y escenario de simulación de la línea a partir de la base de datos
DatosLinea1 = LeerDatos.DatosDC('Linea1', '2017-01-01 00:00:00', '2017-01-01 23:59:00', 'localhost', 'modelo_datos_metrosolar','root', '1234')

# Crear objeto linea
Linea1 = RedLineas.Linea('Linea1', DatosLinea1)

# Crear objeto red AC
Cochrane = RedAC.RedAC('Cochrane', DatosCochrane)

# Conectar línea a red AC
Cochrane.addLinea(Linea1)

# Definir escenario de simulación con el diccionario creado anteriormente
Cochrane.DefinirSimulacion(DatosCochrane)

# Definir escenario de simulación con el diccionario creado anteriormente
Linea1.DefinirSimulacion(DatosLinea1)

# Simular operación conjunto de red AC y línea de metro con potencia base Sbase = 1 [MW]
Cochrane.simular(1)

# Diccionario de resultados de las simulaciones
Resultados = Cochrane.saveresults()

# Guardar resultados de simulaciones de red AC en base de datos
GuardarDatos.saveACresults(Cochrane.ID, Resultados, 'sim1', 'localhost', 'modelo_datos_metrosolar','root', '1234')

# Guardar resultados de simulaciones de líneas en base de datos
for lineaID, lineaRes in Resultados['Lineas'].items():
    GuardarDatos.saveDCresults(lineaID, lineaRes, 'sim1', 'localhost', 'modelo_datos_metrosolar','root', '1234')

# Graficar resultados para red AC
Cochrane.plotresults()

# Graficar resultados para red de tracción DC de línea
Linea1.plotresults()