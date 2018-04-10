# -*- coding: utf-8 -*-
# Ejemplo que sirve para linéa de tracción DC independiente
import RedLineas
import LeerDatos
import GuardarDatos

# Crear diccionario con datos y escenario de simulación de la línea a partir de la base de datos
DatosLinea1 = LeerDatos.DatosDC('Linea1', '2017-01-01 00:00:00', '2017-01-01 23:59:00', 'localhost', 'modelo_datos_metrosolar','root', '1234')

# Crear objeto linea
Linea1 = RedLineas.Linea('Linea1', DatosLinea1)

# Definir escenario de simulación con el diccionario creado anteriormente
Linea1.DefinirSimulacion(DatosLinea1)

# Simular operación conjunto de red AC y línea de metro con potencia base Sbase = 1 [MW]
Linea1.simular()

# Guardar resultados de simulaciones de líneas en base de datos
GuardarDatos.saveDCresults(Linea1.ID, Linea1.saveresults(), 'sim1', 'localhost', 'modelo_datos_metrosolar','root', '1234')

# Graficar resultados para red de tracción DC de línea
Linea1.plotresults()