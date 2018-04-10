# -*- coding: utf-8 -*-
import psycopg2

def saveACresults(RedAC, ResData, simID, ServerIP, DatabaseName, User, Password):
    # Crear conexión con servidor y base de datos
    connection = psycopg2.connect(host=ServerIP, port=5432, user=User, password=Password, dbname=DatabaseName)
    # Crear cursor para realizar query
    cur = connection.cursor()

    # Guardar resultados de simulación para cada terminal
    for Bus, BusRes in ResData['Terminales'].items():
        fechas = list()
        V = list()
        delta = list()
        P = list()
        Q = list()
        for fecha, Res in BusRes.items():
            fechas.append(fecha)
            V.append(Res['V'])
            delta.append(Res['delta'])
            P.append(Res['P'])
            Q.append(Res['Q'])
        # Query para intentar recuperar datos la simulación si es que existen
        sql = 'SELECT * FROM resultados_terminales WHERE Sim_ID="{}" AND Term_ID="{}" AND Fecha>"{}" AND Fecha<="{}" AND Red_ID="{}";'.format(simID, Bus, min(fechas), max(fechas), RedAC)
        try:
            # Intentar recuperar datos para verificar si existen
            cur.execute(sql)
            data = cur.fetchall()
            # Existen datos por lo que se deben actualizar
            if data:
                # Crear lista de resultados de simulaciones que se van a actualizar
                newdata = []
                for fecha, i in enumerate(fechas):
                    newdata.append([V[i], delta[i], P[i], Q[i], simID, fecha, Bus, RedAC])
                # Primero tratar de actualizar campos en caso de que ya existan datos
                cur.executemany(
                    'UPDATE resultados_terminales SET V = %s, delta = %s, P = %s, Q = %s WHERE Sim_ID = %s AND Fecha = %s AND Term_ID = %s AND Red_ID = %s;',
                    newdata)
                connection.commit()
            # No existen datos por lo que se deben insertar
            else:
                data = []
                for i, fecha in enumerate(fechas):
                    data.append([simID, fecha, Bus, RedAC, V[i], delta[i], P[i], Q[i]])
                cur.executemany(
                    'INSERT INTO resultados_terminales (Sim_ID, Fecha, Term_ID, Red_ID, V, delta, P, Q) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                    data)
                connection.commit()
        except:
            connection.rollback()

    # Guardar resultados de simulación para cada cable y trafo
    for Branch, BranchRes in ResData['Ramas'].items():
        fechas = list()
        Pf = list()
        Qf = list()
        Ploss = list()
        Qloss = list()
        Loading = list()
        for fecha, Res in BranchRes.items():
            fechas.append(fecha)
            Pf.append(Res['Pf'])
            Qf.append(Res['Qf'])
            Ploss.append(Res['Ploss'])
            Qloss.append(Res['Qloss'])
            Loading.append(Res['Loading'])
        # Query para intentar recuperar datos la simulación si es que existen
        sql = 'SELECT * FROM resultados_branch WHERE Sim_ID="{}" AND Branch_ID="{}" AND Fecha>"{}" AND Fecha<="{}" AND Red_ID="{}";'.format(
            simID, Branch, min(fechas), max(fechas), RedAC)
        try:
            # Intentar recuperar datos para verificar si existen
            cur.execute(sql)
            data = cur.fetchall()
            # Existen datos por lo que se deben actualizar
            if data:
                # Crear lista de resultados de simulaciones que se van a actualizar
                newdata = []
                for fecha, i in enumerate(fechas):
                    newdata.append([Pf[i], Qf[i], Ploss[i], Qloss[i], Loading[i], simID, fecha,Branch, RedAC])
                # Primero tratar de actualizar campos en caso de que ya existan datos
                cur.executemany('UPDATE resultados_branch SET Pf = %s, Qf = %s, Ploss = %s, Qloss = %s, Loading = %s WHERE Sim_ID = %s AND Fecha = %s AND Branch_ID = %s AND Red_ID = %s;', newdata)
                connection.commit()
            # No existen datos por lo que se deben insertar
            else:
                data = []
                for i, fecha in enumerate(fechas):
                    data.append([simID, fecha, Branch, RedAC, Pf[i], Qf[i], Ploss[i],Qloss[i], Loading[i]])
                cur.executemany(
                    'INSERT INTO resultados_branch (Sim_ID, Fecha, Branch_ID, Red_ID, Pf, Qf, Ploss, Qloss, Loading) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    data)
                connection.commit()
        except:
            connection.rollback()

    # Cerrar cursor
    cur.close()
    # Cerrar conexión a base de datos
    connection.close()

def saveDCresults(Linea, ResData, simID, ServerIP, DatabaseName, User, Password):
    # Crear conexión a servidor y base de datos
    connection = pymysql.Connect(host=ServerIP, port=3306, user=User, passwd=Password, db=DatabaseName)

    # Crear cursor para realizar query
    cur = connection.cursor(pymysql.cursors.DictCursor)

    # Guardar parámetros de simulación realizada
    try:
        cur.execute(
            'INSERT INTO resumen_simulaciones_dc (Sim_ID, E_SER, E_Trenes, E_Perdidas, E_almacenable) VALUES ("{}","{}","{}",{},{}) ON DUPLICATE KEY UPDATE E_SER=VALUES(E_SER), E_Trenes=VALUES(E_Trenes), E_Perdidas=VALUES(E_Perdidas), E_almacenable=VALUES(E_almacenable);'.format(
simID, sum(ResData['General']['PSER']), sum(ResData['General']['PTrenes']), sum(ResData['General']['Perdidas']), -sum(ResData['General']['Pexc'])))
        connection.commit()
    except:
        connection.rollback()
        raise ValueError("no se definieron parámetros válidos para registrar la simulación.")

    # Guardar resultados de simulación para cada tren
    for Tren, TrenRes in ResData['Trenes'].items():
        fechas = list()
        V = list()
        P = list()
        for fecha, Res in TrenRes.items():
            fechas.append(fecha)
            V .append(Res['V'])
            P.append(Res['P'])
        # Query para intentar recuperar datos la simulación si es que existen
        sql = 'SELECT * FROM resultados_elementos_dc WHERE Sim_ID="{}" AND Elemento_ID="{}" AND Fecha>"{}" AND Fecha<="{}";'.format(simID, Tren, min(fechas), max(fechas))
        try:
            # Intentar recuperar datos para verificar si existen
            cur.execute(sql)
            data = cur.fetchall()
            # Existen datos por lo que se deben actualizar
            if data:
                # Crear lista de resultados de simulaciones que se van a actualizar
                newdata = []
                for fecha, i in enumerate(fechas):
                    newdata.append([V[i], P[i], simID, fecha, Tren, Linea])
                # Primero tratar de actualizar campos en caso de que ya existan datos
                cur.executemany(
                    'UPDATE resultados_elementos_dc SET V = %s, P = %s WHERE Sim_ID = %s AND Fecha = %s AND Elemento_ID = %s AND Linea_ID = %s;',
                    newdata)
                connection.commit()
            # No existen datos por lo que se deben insertar
            else:
                data = []
                for i, fecha in enumerate(fechas):
                    data.append([simID, fecha, Tren, Linea, V[i], P[i]])
                cur.executemany(
                    'INSERT INTO resultados_elementos_dc (Sim_ID, Fecha, Elemento_ID, Linea_ID, V, P) VALUES (%s, %s, %s, %s, %s, %s)',
                    data)
                connection.commit()
        except:
            connection.rollback()

    # Guardar resultados de simulación para cada SER
    for SER, SERRes in ResData['SER'].items():
        fechas = list()
        V = list()
        P = list()
        for fecha, Res in SERRes.items():
            fechas.append(fecha)
            V .append(Res['V'])
            P.append(Res['P'])
        # Query para intentar recuperar datos la simulación si es que existen
        sql = 'SELECT * FROM resultados_elementos_dc WHERE Sim_ID="{}" AND Elemento_ID="{}" AND Fecha>"{}" AND Fecha<="{}";'.format(
            simID, SER, min(fechas), max(fechas))
        try:
            # Intentar recuperar datos para verificar si existen
            cur.execute(sql)
            data = cur.fetchall()
            # Existen datos por lo que se deben actualizar
            if data:
                # Crear lista de resultados de simulaciones que se van a actualizar
                newdata = []
                for fecha, i in enumerate(fechas):
                    newdata.append([V[i], P[i], simID, fecha, SER, Linea])
                # Primero tratar de actualizar campos en caso de que ya existan datos
                cur.executemany(
                    'UPDATE resultados_elementos_dc SET V = %s, P = %s WHERE Sim_ID = %s AND Fecha = %s AND Elemento_ID = %s AND Linea_ID = %s;',
                    newdata)
                connection.commit()
            # No existen datos por lo que se deben insertar
            else:
                data = []
                for i, fecha in enumerate(fechas):
                    data.append([simID, fecha, SER, Linea, V[i], P[i].item()])
                cur.executemany(
                    'INSERT INTO resultados_elementos_dc (Sim_ID, Fecha, Elemento_ID, Linea_ID, V, P) VALUES (%s, %s, %s, %s, %s, %s)',
                    data)
                connection.commit()
        except:
            connection.rollback()

    # Guardar resultados de simulación para cada PV
    for PVdc, PVdcRes in ResData['PV'].items():
        fechas = list()
        V = list()
        P = list()
        for fecha, Res in PVdcRes.items():
            fechas.append(fecha)
            V .append(Res['V'])
            P.append(Res['P'])
        # Query para intentar recuperar datos la simulación si es que existen
        sql = 'SELECT * FROM resultados_elementos_dc WHERE Sim_ID="{}" AND Elemento_ID="{}" AND Fecha>"{}" AND Fecha<="{}";'.format(
            simID, PVdc, min(fechas), max(fechas))
        try:
            # Intentar recuperar datos para verificar si existen
            cur.execute(sql)
            data = cur.fetchall()
            # Existen datos por lo que se deben actualizar
            if data:
                # Crear lista de resultados de simulaciones que se van a actualizar
                newdata = []
                for fecha, i in enumerate(fechas):
                    newdata.append([V[i], P[i], simID, fecha, PVdc, Linea])
                # Primero tratar de actualizar campos en caso de que ya existan datos
                cur.executemany(
                    'UPDATE resultados_elementos_dc SET V = %s, P = %s WHERE Sim_ID = %s AND Fecha = %s AND Elemento_ID = %s AND Linea_ID = %s;',
                    newdata)
                connection.commit()
            # No existen datos por lo que se deben insertar
            else:
                data = []
                for i, fecha in enumerate(fechas):
                    data.append([simID, fecha, PVdc, Linea, V[i], P[i]])
                cur.executemany(
                    'INSERT INTO resultados_elementos_dc (Sim_ID, Fecha, Elemento_ID, Linea_ID, V, P) VALUES (%s, %s, %s, %s, %s, %s)',
                    data)
                connection.commit()
        except:
            connection.rollback()

    # Cerrar cursor
    cur.close()
    # Cerrar conexión a base de datos
    connection.close()