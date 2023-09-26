from datetime import datetime 
import pandas as pd
import sqlalchemy as db
import numpy as np
import pyodbc

if __name__ == "__main__":
    #Control execution time
    start = datetime.now()


    #Global variable
    period = '202309'

    conn = pyodbc.connect(
        'Driver={SQL Server};'
        'Server= {CFERNANDEZSAXP\SERVIDORRETAIL};' #YOUR_SERVER
        'Database={BDTCADQ};' #Your DB_BASE
        'Trusted_Connection=yes'
    )
    consulta = conn.cursor()
    consulta.execute('''
        DELETE ACT_TABLAS_GCV;
        
        --GIRU RETENCIONES
        IF 
        EXISTS (SELECT * FROM RA_GIRU_RETEN_REPORT where Mes_CE2 = '''+period+''')
            INSERT INTO ACT_TABLAS_GCV
            SELECT Mes_CE2 PERIODO,'RA_GIRU_RETEN_REPORT' TABLA,
            CAST(max(Dia_CE) AS VARCHAR) AS max_dia, 
            'tabla no vacia' AS ESTADO
            FROM RA_GIRU_RETEN_REPORT
            WHERE Mes_CE2 =''' +period+'''
            GROUP BY Mes_CE2
        ELSE 
            INSERT INTO ACT_TABLAS_GCV
            SELECT '''+period+''' PERIODO,'RA_GIRU_RETEN_REPORT' TABLA , '-' max_dia, 'tabla vacia' AS ESTADO ;
        
        -- SEGUROS 
        IF EXISTS (SELECT * FROM Efectividad_Seguros where COD_MES = '''+period+''')
            INSERT INTO ACT_TABLAS_GCV
            SELECT COD_MES AS PERIODO,
            'Efectividad_Seguros' TABLA , 
            CAST(max(SOLUCION) AS VARCHAR) AS max_dia,
            'tabla no vacia' AS ESTADO
            FROM Efectividad_Seguros
            WHERE COD_MES = '''+period+''' 
            GROUP BY COD_MES
        ELSE
            INSERT INTO ACT_TABLAS_GCV
            SELECT  '''+period+''' PERIODO, 'Efectividad_Seguros' TABLA , '-' max_dia,'tabla vacia' AS ESTADO;

        -- CTS
        IF EXISTS (SELECT * FROM Efectividad_CTS where MES = '''+period+''')
            INSERT INTO ACT_TABLAS_GCV
            SELECT 
            MES AS PERIODO,
            'Efectividad_CTS' TABLA , 
            CAST(max(SOLUCION) AS VARCHAR) AS max_dia,
            'tabla no vacia' AS ESTADO 
            FROM Efectividad_CTS
            WHERE MES ='''+period+'''
            GROUP BY MES
        ELSE
            INSERT INTO ACT_TABLAS_GCV
            SELECT '''+period+''' PERIODO, 'Efectividad_CTS' TABLA , '-' max_dia,'tabla vacia' AS ESTADO;
        ''')

    consulta.commit()
    consulta.close()

    #Read the query of table
    query_temp = '''
    SELECT * FROM ACT_TABLAS_GCV
    '''
    df_act= pd.read_sql_query(query_temp,conn)

    #Description of mail
    descripton='''
    <p>
    Hola team, se reporta el estado de las tablas:
    </p>
    <br>
    '''

    #Head of table in HTML
    head= '''       
        <table width="520" style=" border-collapse: collapse; margin-left: auto; margin-right: auto;text-align: center;" border="0">
            <tbody>
                <tr style="height:18px;">
                    <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white">Periodo</th>
                    <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white">Tabla</th>
                    <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white">Actualización</th>
                    <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white">Estado</th>
                </tr>
                <!--<tr style="height:10px;"></tr>-->
            '''

    #Convert the dataframe to numpy
    df_list = df_act.to_numpy().tolist()

    #function that help you for search a word in specific
    def srch(word):
        # Encontrar la sublista que contiene la palabra
        sublista_con_palabra = np.array([fila for fila in df_list if word in fila])
        final=''
        for word_search in sublista_con_palabra[0]:
            data_col = '''<td style ="border-style: groove;">'''+word_search+'''</td>'''
            temp = data_col
            final +=temp
        return final

    tablas = ["RA_GIRU_RETEN_REPORT","Efectividad_Seguros","Efectividad_CTS"]

    table_giru = srch('RA_GIRU_RETEN_REPORT')
    table_seg = srch('Efectividad_Seguros')
    table_cts = srch('Efectividad_CTS')


    body = '''
            <tr>
                '''+table_giru+'''
            </tr>

            <tr>
                '''+table_seg+'''
            </tr>

            <tr>
                '''+table_cts+'''
            </tr>
    </table>
    '''

    footer='''
    <br><br>
    <p style="font-family:Arial;font-weight: bold;font-size:12px">Saludos
    <br>Desarrollo de Negocios</p> 
    <img src="https://i.imgur.com/gqh4ipx.png"/>
    '''

    x=descripton+head+body+footer



    #Mandamos el mail mediante SP
    Destinatarios='rochoaca@intercorp.com.pe;jsalcedob@intercorp.com.pe;jvelsquez@intercorp.com.pe'
    Copias ='''gtamaram@intercorp.com.pe;dcedanoo@intercorp.com.pe;ntocasc@intercorp.com.pe;vortegaa@intercorp.com.pe;kesteban@intercorp.com.pe;acuzcoab@intercorp.com.pe'''
    Subject='Informacion TLV - Retencion -  ' + period
    try:
        server_name = 'CFERNANDEZSAXP\SERVIDORRETAIL' 
        database_name = 'BDTCADQ' 
        driver = 'SQL Server' 
        username='usuario_dn'
        password='escritura'
        link = f'DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        engine=link
        conection = pyodbc.connect(engine)
        cursor_2 = conection.cursor()
        cursor_2.execute("exec [dbo].[GCVE_ALERTA_GENERICA_CC] ?,?,?,?", x, Subject , Destinatarios,Copias)
        cursor_2.commit()
        cursor_2.close()
        print('Enviado')
    except Exception: # as e:
        print('Error could not connet to fernandez')

    print('Execution time : ' ,datetime.now() - start)