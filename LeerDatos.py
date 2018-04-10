# -*- coding: utf-8 -*-

import warnings
import psycopg2
from psycopg2.extras import DictCursor

# Funcion para leer datos de atributos de objetos de red AC
def LeerAtributosAC(RedACID, Data, db_connection):
    # Crear cursor para realizar query como diccionario para evitar problemas si se cambia orden de columnas
    # en base de datos
    cur = db_connection.cursor()
    # Crear diccionario de datos para red ac
    Atributos = dict()
    # Resetear diccionario de datos generales
    Data['Atributos'] = dict()
    # Crear terminales en lado AC para red respectiva
    sql = cur.mogrify('SELECT * FROM atributos_terminales WHERE Red_ID=%s;', (RedACID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        results = cur.fetchall()
        # Crear diccionario para datos de terminales
        TermData = dict()
        for row in results:
            TermData[row['term_id']]={'Vnom': row['vnom'], 'Save': row['save']}
        
        # Guardar diccionario con atributos de terminales en diccionario de datos de red AC
        Atributos['TermData'] = TermData
    except:
        raise ValueError('No se encuentran terminales para la red especificada. No es posible construir la topología')


    # Crear transformadores de red AC
    sql = cur.mogrify('SELECT * FROM atributos_trafos WHERE red_id=%s AND en_operacion=True;', (RedACID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        results = cur.fetchall()
        # Crear diccionario para datos de terminales
        TrafoData = dict()
        for row in results:
            TrafoID = row['trafo_id']
            # Obtener ID de terminal de conexión para primario de cada trafo
            HVTermID = row['hvterm_id']
            # Obtener ID de terminal de conexión para secundario de cada trafo
            LVTermID = row['lvterm_id']
            # Recuperar otros atributos de trafos
            Capacidad = set(row['snom'] for row in results).pop()
            Resistencia = set(row['resistencia'] for row in results).pop()
            Reactancia = set(row['reactancia'] for row in results).pop()
            # Crear diccionario para datos de transformadores
            TrafoData[TrafoID] = {'Snom': Capacidad, 'R': Resistencia, 'X': Reactancia, 'HV': HVTermID, 'LV': LVTermID, 'Save': row['save']}
        
        # Guardar diccionario con atributos de trafos en diccionario de datos de red AC
        Atributos['TrafoData'] = TrafoData
    except:
        raise ValueError('No se encuentran transformadores para la línea especificada, no se puede construir topología')

    # Crear SAF y conectarlos a sus respectivos terminales
    sql = cur.mogrify('SELECT * FROM atributos_saf WHERE red_id=%s AND en_operacion=True;', (RedACID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        results = cur.fetchall()
        # Crear diccionario para datos de SAF
        SAFData = dict()
        for row in results:
            # Obtener ID de SAF
            SAFID = row['saf_id']
            # Obtener ID de terminal de conexión para SAF
            TermID = row['term_id']
            ConsumoID = row['consumo_id']
            Vac = row['vac']
            Capacidad = row['snom']
            # Crear diccionario para datos de SAF
            SAFData[SAFID] = {'Vac': Vac, 'Snom': Capacidad, 'Consumo': ConsumoID, 'TermID': TermID}
        
        # Guardar diccionario con atributos de SAF en diccionario de datos de red AC
        Atributos['SAFData'] = SAFData
    except psycopg2.Error as e:
        warnings.warn('No se encuentran SAF para la línea especificada')

    # Crear CDC de la linea
    sql = cur.mogrify('SELECT * FROM atributos_cdc WHERE red_id=%s;', (RedACID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        results = cur.fetchall()
        # Crear diccionario para datos de CDC
        CDCData = dict()
        for row in results:
            CDCID = row['cdc_id']
            # Obtener ID de terminal de conexión para SAF
            TermID = row['term_id']
            # Recuperar otros atributos de CDC
            Vac = row['vnom']
            Capacidad = row['snom']
            # Crear diccionario para datos de CDC
            CDCData[CDCID]={'Vac': Vac, 'Snom': Capacidad,'TermID': TermID, 'Save': row['save']}
        
        # Guardar diccionario con atributos de CDC en diccionario de datos de red AC
        Atributos['CDCData'] = CDCData
    except:
        raise ValueError('No se encuentran CDC para la línea especificada, no se puede construir topología.')

    # Crear módulos PV en lado AC
    # Obtener PVAC de la línea
    sql = cur.mogrify('SELECT * FROM atributos_pvac WHERE red_id=%s AND en_operacion=True;', (RedACID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        ListaPVAC = cur.fetchall()
        # Crear diccionario para datos de módulos PV en red ac
        PVacData = dict()
        for row in ListaPVAC:
            # Obtener ID de cada módulo
            PVACID = row['pvac_id']
            voltaje = row['vnom']
            capacidad = row['snom']
            TermID = row['term_id']
            cosphi = row['cosphi']
            PerfilPVAC = row['perfil_pv']
            # Agregar PVdc sin perfil, el cual se agrega una vez se realiza la simulación filtrando por fecha
            PVacData[PVACID] = {'Vac': voltaje, 'Snom': capacidad, 'Perfil': PerfilPVAC, 'cosphi': cosphi, 'TermID': TermID}
        
        # Guardar diccionario con atributos de módulos PV en lado ac en diccionario de datos de red AC
        Atributos['PVacData'] = PVacData
    except:
        warnings.warn('No se encontraron PV para la red especificada.')

    # Crear cables para conectar terminales de la linea
    sql = cur.mogrify('SELECT * FROM atributos_cables WHERE red_id=%s AND en_operacion=True;', (RedACID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        results = cur.fetchall()
        # Crear diccionario para datos de cables de red ac
        CablesData = dict()
        for row in results:
            CableID = row['cable_id']
            # Obtener ID de terminal de conexión para SAF
            Term1ID = row['term_id1']
            Term2ID = row['term_id2']
            # Recuperar otros atributos de cables
            largo = row['largo']
            capacidad = row['snom']
            resistencia= row['resistencia']
            reactancia = row['reactancia']
            capacitancia = row['capacitancia']
            # Agregar cable
            CablesData[CableID] = {'Snom': capacidad, 'L': largo, 'r':resistencia, 'x': reactancia, 'c': capacitancia, 'Term1': Term1ID, 'Term2': Term2ID, 'Save': row['save']}
        
        # Guardar diccionario con atributos de cables en diccionario de datos de red AC
        Atributos['CablesData'] = CablesData
    except:
        raise ValueError('No se encuentran cables disponibles para la línea especificada')

    # Cerrar cursor
    cur.close()

    # Guardar diccionario con atributos en diccinario de datos generales
    Data['Atributos'] = Atributos
    # Retornar diccionario con datos de red AC para constuir objetos
    return Data

# Funcion para leer datos de atributos de objetos de red AC
def LeerEscenariosAC(Data, Fecha_ini, Fecha_fin, db_connection):
    # Crear cursor para realizar query
    cur = db_connection.cursor()
    # Resetear diccionario de datos generales
    Data['Escenario'] = dict()
    # Crear diccionario de datos para red ac
    Escenario = dict()
    # Si hay una red con atributos definida en el diccionario de datos, empezar a rescatar datos de simulación
    if Data['Atributos']:
        # Extraer y asignar perfiles PVac
        PerfilPVSim = dict()
        for PVacID, PVac in Data['Atributos']['PVacData'].items():
            Perfil = PVac['Perfil']
            sql = cur.mogrify('SELECT * FROM perfiles_pv WHERE Perfil_ID=%s AND Fecha>=%s AND Fecha<%s ORDER BY Fecha;',
                              (Perfil, Fecha_ini, Fecha_fin))
            try:
                # Ejecutar query
                cur.execute(sql)
                # Obtener perfiles de módulos PV
                results = cur.fetchall()
                PerfilPV = dict()
                for row in results:
                    PerfilPV[row['fecha']] = {'P': row['p']}
                # Guardar datos de simulación
                PerfilPVSim[PVacID] = PerfilPV
            except:
                raise ValueError('No se encuentran perfiles disponibles para módulos pv de la línea')
        # Guardar datos en diccionario general para simulación de red AC
        Escenario['Perfiles_PV'] = PerfilPVSim
    
        # Extraer y asignar perfiles SAF
        PerfilSAFSim = dict()
        for SAFID, SAF in Data['Atributos']['SAFData'].items():
            ConsumoID = SAF['Consumo']
            sql = cur.mogrify('SELECT * FROM perfiles_saf WHERE Consumo_ID=%s AND Fecha>=%s AND Fecha<%s ORDER BY Fecha;',
                              (ConsumoID, Fecha_ini, Fecha_fin))
            try:
                # Ejecutar query
                cur.execute(sql)
                # Obtener parametros de la vias y agregarlas a la linea creada
                results = cur.fetchall()
                # Recuperar arreglo de fechas
                PerfilSAF = dict()
                for row in results:
                    PerfilSAF[row['fecha']] = {'P': row['p'], 'Q': row['q']}
                # Guardar datos de simulación
                PerfilSAFSim[SAFID] = PerfilSAF
            except:
                raise ValueError('No se encuentran perfiles de consumo disponibles para SAFs de la línea')
        # Guardar datos en diccionario general para simulación de red AC
        Escenario['Perfiles_SAF'] = PerfilSAFSim

    # Cerrar cursor
    cur.close()

    # Guardar diccionario con datos para simulación en diccinario de datos generales
    Data['Escenario'] = Escenario
    # Retornar diccionario con datos de red AC para simulación
    return Data

# Funcion para leer datos de atributos de objetos de red DC
def LeerAtributosDC(LineaID, Data, db_connection):
    # Crear cursor para realizar query
    cur = db_connection.cursor()
    # Resetear diccionario de datos generales
    Data['Atributos'] = dict()
    # Crear diccionario de datos para red ac
    Atributos = dict()

    # Verificar que línea especificada existe en base de datos
    sql = cur.mogrify('SELECT * FROM atributos_lineas WHERE Linea_ID=%s;', (LineaID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la linea
        results = cur.fetchone()
        LineaData = dict()
        LineaData[results['linea_id']] = {'Vnom': results['vnom']}
        Atributos['LineaData'] = LineaData
    except:
        raise ValueError('No se encuentra la línea especificada en la base de datos')

    # VIAS
    sql = cur.mogrify('SELECT * FROM atributos_vias WHERE Linea_ID=%s;',(LineaID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        results = cur.fetchall()
        # Crear diccionario para datos de vías de la línea
        ViasData = dict()
        # Guardar en diccionario
        for row in results:
            ViasData[row['via_id']]= {'L': row['largo'], 'r': row['resistividad'], 'Nrieles': row['nrieles']}
        # Guardar diccionario con atributos de vías en diccionario de datos de línea para tracción
        Atributos['ViasData'] = ViasData
    except:
        raise ValueError('No se encuentran vias para la línea especificada')

    # TRENES
    sql = cur.mogrify('SELECT * FROM atributos_trenes WHERE Linea_ID=%s AND En_operacion=True;',(LineaID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        results = cur.fetchall()
        # Crear diccionario para datos de vías de la línea
        TrenesData = dict()
        # Guardar datos en diccionario
        for row in results:
            # Agregar Tren
            TrenesData[row['tren_id']] = {'Via': row['via_id'], 'Status': row['en_operacion'], 'Save': row['save']}
        # Guardar diccionario con atributos de trenes en diccionario de datos de línea para tracción
        Atributos['TrenesData'] = TrenesData
    except:
        raise ValueError('No se encontraron trenes para la línea especificada')

    # SER
    # Obtener SER de la línea
    sql = cur.mogrify('SELECT Elemento_ID FROM lista_elementos_dc WHERE Linea_ID=%s AND Tipo=\'SER\';', (LineaID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        ListaSER = cur.fetchall()
        # Crear diccionario para datos de SER de la línea
        SERData = dict()
        for row in ListaSER:
            # Obtener ID de cada SER
            SERID = row['elemento_id']
            # Sacar vias que alimenta cada SER
            sql = 'SELECT * FROM atributos_ser WHERE SER_ID="{}" AND En_operacion=True;'.format(SERID)
            try:
                cur.execute(sql)
                results = cur.fetchall()
                # Obtener IDs de Vias alimentadas por cada SER
                ViasIDSER = set(row['via_id'] for row in results)
                # recuperar objetos de vias respectivos
                # RECORDATORIO: POR AHORA LOS SER ESTAN PENSADOS PARA ALIMENTAR VARIAS VIAS, POR LO QUE PODRIAN
                # TENER MAS DE UNA POSICION, RESISTENCIA, ETC... PERO EL CODIGO ESTA CONSIDERANDO LOS MISMOS
                # PARAMETROS PARA CADA VIA (pendiente para próxima actualización)
                # Obtener ID de terminal de conexión para cada SER
                TermID = set(row['term_id'] for row in results).pop()
                # Definir el resto de los parámetros de cada SER
                PosSER = set(row['posvia'] for row in results).pop()
                VacSER = set(row['vac'] for row in results).pop()
                VdcSER = set(row['vdc'] for row in results).pop()
                RSER = set(row['resistencia'] for row in results).pop()
                Save = set(row['save'] for row in results).pop()
                # Guardar datos en diccionario
                SERData[SERID] = {'Vias': ViasIDSER, 'pos': PosSER, 'Vac': VacSER, 'Vdc': VdcSER, 'R': RSER, 'TermID': TermID, 'Save': Save}
            except:
                pass
        # Guardar diccionario con atributos de SER en diccionario de datos de línea para tracción
        Atributos['SERData'] = SERData
    except:
        raise ValueError('No se encontraron SER para la línea especificada, revisar definición de datos')

    # módulos PV en lado de tracción
    # Obtener PVDC de la línea
    sql = cur.mogrify('SELECT Elemento_ID FROM lista_elementos_dc WHERE Linea_ID=%s AND Tipo="PVDC";', (LineaID,))
    try:
        # Ejecutar query
        cur.execute(sql)
        # Obtener parametros de la vias y agregarlas a la linea creada
        ListaPVDC = cur.fetchall()
        # Crear diccionario para datos de módulos PV de la línea
        PVdcData = dict()
        for row in ListaPVDC:
            # Obtener ID de cada SER
            PVDCID = row['elemento_id']
            # Sacar vias que alimenta cada SER
            sql = 'SELECT * FROM atributos_pvdc WHERE Linea_ID="{}" AND En_operacion=True;'.format(LineaID)
            try:
                cur.execute(sql)
                results = cur.fetchall()
                # Obtener IDs de Vias alimentadas por cada SER
                ViasIDPVDC = set(row['via'] for row in results)
                # RECORDATORIO: POR AHORA LOS PV ESTAN PENSADOS PARA ALIMENTAR VARIAS VIAS, POR LO QUE PODRIAN
                # TENER MAS DE UNA POSICION, RESISTENCIA, ETC... PERO EL CODIGO ESTA CONSIDERANDO LOS MISMOS
                # PARAMETROS PARA CADA VIA (pendiente para próxima actualización)
                PosPVDC = set(row['posvia'] for row in results).pop()
                PerfilPVDC = set(row['perfilpv'] for row in results).pop()
                Save = set(row['save'] for row in results).pop()
                # Agregar PVdc sin perfil, el cual se agrega una vez se realiza la simulación filtrando por fecha
                PVdcData[PVDCID] = {'Vias': ViasIDPVDC, 'pos': PosPVDC, 'Perfil': PerfilPVDC, 'Save': Save}
            except:
                pass
        # Guardar diccionario con atributos de módulos PV en diccionario de datos de línea para tracción
        Atributos['PVdcData'] = PVdcData
    except:
        warnings.warn('No se encontraron PV para la línea especificada, revisar definición de datos')

    # Cerrar cursor
    cur.close()

    # Guardar diccionario con datos para simulación en diccinario de datos generales
    Data['Atributos'] = Atributos
    # Retornar diccionario con datos de red AC para simulación
    return Data

# Funcion para leer datos de atributos de objetos de red AC
def LeerEscenariosDC(LineaID, Data, Fecha_ini, Fecha_fin, db_connection):
    # Crear cursor para realizar query
    cur = db_connection.cursor()
    # Resetear diccionario de datos generales
    Data['Escenario'] = dict()
    # Crear diccionario de datos para red ac
    Escenario = dict()

    # Extraer bitacora de trenes
    # Crear diccionario de datos de simulación para trenes
    BitacoraTren = dict()
    for TrenID, tren in Data['Atributos']['TrenesData'].items():
        sql = cur.mogrify('SELECT * FROM bitacora_trenes where Linea_ID=%s and Fecha>=%s and Fecha<%s and Tren_ID=%s order by Fecha;',
                          (LineaID, Fecha_ini, Fecha_fin, TrenID))
        try:
            # Ejecutar query
            cur.execute(sql)
            # Obtener parametros de la vias y agregarlas a la linea creada
            results = cur.fetchall()
            # Obtener lista de posiciones para simulación
            Bitacora = dict()
            for row in results:
                Bitacora[row['fecha']] = {'pos': row['posicion'], 'P': row['potencia']}
            # Guardar bitácora de cada tren
            BitacoraTren[TrenID] = Bitacora
        except Exception as e:
            raise e
            raise ValueError('No se encuentran bitacoras disponibles para trenes de la línea')
    # Guardar bitácoras en diccionario de perfiles para simulaciones
    Escenario['Trenes'] = BitacoraTren

    # Extraer y asignar perfiles PVdc
    # Crear diccionario de datos de simulación para trenes
    PerfilPV = dict()
    for PVID, PVdcdata in Data['Atributos']['PVdcData'].items():
        sql = cur.mogrify('SELECT * FROM perfiles_pv where Fecha>=%s and Fecha<%s and Perfil_ID=%s order by Fecha;',
                          (Fecha_ini, Fecha_fin, PVdcdata['Perfil']))
        try:
            # Ejecutar query
            cur.execute(sql)
            # Obtener parametros de la vias y agregarlas a la linea creada
            results = cur.fetchall()
            # Recuperar arreglo de fechas
            fechas = [row["fecha"] for row in results]
            # Obtener lista perfilPV para simulación
            MPPT = [row["p"] for row in results]
            PerfilPV[PVID] = {'Fechas': fechas, 'P': MPPT}
        except:
            raise ValueError('No se encuentran perfiles disponibles para módulos pv de la línea')
    # Guardar bitácoras en diccionario de perfiles para simulaciones
    Escenario['PV'] = PerfilPV

    # Cerrar cursor
    cur.close()

    # Guardar diccionario con datos para simulación en diccinario de datos generales
    Data['Escenario'] = Escenario
    # Retornar diccionario con datos de red AC para simulación
    return Data

# Función main para ejecutar métodos para lectura de datos desde servidor
def DatosDC(LineaID, Fecha_ini, Fecha_fin, db_connection):
    Data = dict()

    LeerAtributosDC(LineaID, Data, db_connection)

    LeerEscenariosDC(LineaID, Data, Fecha_ini, Fecha_fin, db_connection)

    return Data

# Función main para ejecutar métodos para lectura de datos desde servidor
def DatosAC(RedACID, Fecha_ini, Fecha_fin, db_connection):
    Data = dict()

    LeerAtributosAC(RedACID, Data, db_connection)

    LeerEscenariosAC(Data, Fecha_ini, Fecha_fin, db_connection)

    return Data