# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 18:32:24 2022

@author: BP2982 // Versión usable
"""
from datetime import datetime 
import pandas as pd
import sqlalchemy as db
import numpy as np
import pyodbc

start = datetime.now()

conn = pyodbc.connect('Driver={Teradata Database ODBC Driver 16.20};'
                      'DBCNAME={10.11.240.31};'
                      'UID={APP_PlanContrGestCanales};'
                      'PWD={frBNeXosBa}')

periodo = '202306'

consulta = conn.cursor()
conn.autocommit = True
consulta.execute('''
    CREATE VOLATILE TABLE CG_CTSBO_ALERTA AS (
SELECT
    CASE
			WHEN ZONA  IN ('ZONA NORTE PIURA-TUMBES', 'ZONA NORTE','ZONA ESTE','REGIÓN NORTE','ZONA PIURA-TUMBES','ZONA CENTRO ANDINO') THEN 'LILEY' --CARDENAS DAVILA LILEY MARIA
	        --WHEN B.NUEVA_ZONA IN ('ZONA OESTE','ZONA CENTRO ANDINO','ZONA CENTRO ICA') THEN 'B24789'
	        WHEN ZONA IN ('ZONA CENTRO ICA','ZONA CENTRO','ZONA NORTE CHICLAYO-CAJAM','ZONA SUR INCA','ZONA MODERNA','VZONA NORTE CHICO ANCASH TRUJILLO') THEN 'LIZ'--HUAYANA RUIZ LIZ RAQUEL
	        WHEN ZONA IN ('ZONA SUR','ZONA CENTRO ORIENTE','ZONA SUR GRANDE','ZONA OESTE')THEN 'OLGA' --CONTEÑA HUAPAYA OLGA GRACIELA
	 	ELSE NULL
    END AS RESPONSABLE, ZONA, ZEROIFNULL(SUM(SOLICITUDES)) AS AVANCE, SUM(META) AS META, ROUND(ZEROIFNULL((SUM(SOLICITUDES)/SUM(META))*100)) AS ALCANCE
    --select *
    FROM DLAB_DESNEGRET.CTS_AVANCE_TIENDAS
    WHERE CODMES ='''+periodo+''' and META IS NOT NULL AND ESTADO_TIENDA = 'Activa'
    GROUP BY ZONA     
   --ORDER BY RESPONSABLE 
    union
                                
    select a.responsable,'TOTAL' zona,sum(a.avance),sum(a.meta), ROUND((sum(a.avance)/sum(a.meta))*100)
    from (
            SELECT
                CASE
            			WHEN ZONA  IN ('ZONA NORTE PIURA-TUMBES', 'ZONA NORTE','ZONA ESTE','REGIÓN NORTE','ZONA PIURA-TUMBES','ZONA CENTRO ANDINO') THEN 'LILEY' --CARDENAS DAVILA LILEY MARIA
            	        --WHEN B.NUEVA_ZONA IN ('ZONA OESTE','ZONA CENTRO ANDINO','ZONA CENTRO ICA') THEN 'B24789'
            	        WHEN ZONA IN ('ZONA CENTRO ICA','ZONA CENTRO','ZONA NORTE CHICLAYO-CAJAM','ZONA SUR INCA','ZONA MODERNA','VZONA NORTE CHICO ANCASH TRUJILLO') THEN 'LIZ'--HUAYANA RUIZ LIZ RAQUEL
            	        WHEN ZONA IN ('ZONA SUR','ZONA CENTRO ORIENTE','ZONA SUR GRANDE','ZONA OESTE')THEN 'OLGA' --CONTEÑA HUAPAYA OLGA GRACIELA
            	 	ELSE NULL
                END AS RESPONSABLE, ZONA, ZEROIFNULL(SUM(SOLICITUDES)) AS AVANCE, SUM(META) AS META, ROUND(ZEROIFNULL((SUM(SOLICITUDES)/SUM(META))*100)) AS ALCANCE
                --select *
                FROM DLAB_DESNEGRET.CTS_AVANCE_TIENDAS
                WHERE CODMES ='''+periodo+'''
                and META IS NOT NULL 
                AND ESTADO_TIENDA = 'Activa'
                GROUP BY ZONA
                --ORDER BY RESPONSABLE 
                ) A
                                group by a.responsable
) WITH DATA PRIMARY INDEX(
    RESPONSABLE
) ON COMMIT PRESERVE ROWS;
    ''')
    
query_temporal = '''
SELECT * FROM CG_CTSBO_ALERTA
order by responsable, Avance, meta
'''

data_avance = pd.read_sql_query(query_temporal,conn)

avance_cts= round(100*(data_avance.loc[data_avance["ZONA"] !="TOTAL","AVANCE" ].sum()/data_avance.loc[data_avance["ZONA"] !="TOTAL","META" ].sum()))
data_avance = data_avance.append({'RESPONSABLE':"ZONA TOTAL",'ZONA':"TOTAL",'AVANCE':data_avance.loc[data_avance["ZONA"] !="TOTAL","AVANCE" ].sum(),'META':data_avance.loc[data_avance["ZONA"] !="TOTAL","META" ].sum(),'ALCANCE':avance_cts},ignore_index=True)

fechamax = pd.read_sql('''SELECT MAX(PROVISIONAL_DEPOSITO_FECHA) AS FECHA FROM DLAB_DESNEGRET.CTS_TRACKING 
                       WHERE origen_final='TIENDA' AND ESTADO_ACTUAL <> '1.En Tramite'
                       ''',conn)
hola = fechamax.to_numpy()
temp_str = hola.astype(str)
fecham = temp_str[0][0]
objetoTiempo = datetime.strptime(fecham, '%Y-%m-%d')
FechaMax = objetoTiempo.strftime('%d-%m-%Y')




def image(var):
    if var<60:
        return('<img src="https://i.imgur.com/kXG5WcL.png">')#https://i.imgur.com/IpkA0VE.png
    elif var<90:
        return('<img src="https://i.imgur.com/osr3btT.png">' )# #https://i.imgur.com/C6bbrYm.png
    elif var<1000:
        return('<img src="https://i.imgur.com/JKkt1FZ.png">')#https://i.imgur.com/RmlPCbk.png
    else:
        return('')
# <p style="font-family:Arial;font-size:12px;">
Cabecera= '''       
        <table width="520" style=" border-collapse: collapse; margin-left: auto; margin-right: auto;text-align: center;" border="0">
        <tbody>
        <tr style="height:18px;">
        <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white">Responsable</th>
        <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white;border-right: solid 1px white">Zonas</th>
        <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white">Real</th>
        <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white">Meta</th>
        <th style=" font-family:Arial;font-size:12px;background-color:#05334D;color:white;border-right: solid 1px white">%Timing</th>
        </tr>
        <tr style="height:10px;"></tr>
        '''
        
dotacion = data_avance['RESPONSABLE'].unique().tolist()     
data_avance[['ALCANCE']] = data_avance[['ALCANCE']].astype(int)
data_avance[['META']] = data_avance[['META']].astype(int)
data_avance[['AVANCE']] = data_avance[['AVANCE']].astype(int)

#prueba = round(2.5)


Body_1='''<!DOCTYPE html><html><body><p style="color:black;font-family:Arial;font-size:12px;">
        Equipo,
        <br>Alcanzamos el resumen del avance de solicitudes de CTS a la fecha de corte: '''+FechaMax+'''</p>
        <p style="font-weight: bold;color:black;font-family:Arial;font-size:12px;">Enlace:
        &nbsp;<a href="https://interbankpe-my.sharepoint.com/:f:/g/personal/reportesretail_intercorp_com_pe/Eh5A-cOh7DtHhQuu0J0I4ngB9cWsuESAJJ2rZKb8V9T-1A?e=8KUho9">Reporte CTS</a>
        <br>
        <br>► Avance CTS BackOffice '''+periodo+'''</p>
       '''
x = ''
for dot in dotacion:
    print('siuuuuuuuuuu')
    for n in range(len(data_avance[data_avance.RESPONSABLE==dot])):
        df_temp=data_avance[data_avance.RESPONSABLE==dot]
        df_temp.reset_index(drop=True, inplace=True)
        
        if df_temp.loc[n,'ZONA']=='TOTAL':
            Format='<td style="background-color:#DFDFDF;font-family:Arial;font-size:12px;font-weight: bold;">'
            #Format2='<td style="background-color:#DFDFDF;font-family:Arial;font-size:12px;font-weight: bold;border-right: solid 1px white;">'
        else:
            Format='<td style="font-family:Arial;font-size:12px;">'
            #Format2='<td style=";border-right: solid 1px white;font-family:Arial;font-size:12px;">'
        if n ==0:
            x=x+('''<tr style="color:black;font-family:Arial;font-size:12px;background-color:#f2f2f2;border-radius:20px;">'''+
            '''<td style="font-weight: bold;font-family:Arial;font-size:12px;color:black;background-color:#f2f2f2" '''+' rowspan="'+str(int(len(df_temp[df_temp.RESPONSABLE==dot])+0))+'">'+
            '<p>'+dot+'</p></td>')
        else:
            x=x+'<tr style="color:black;background-color:#f2f2f2;font-family:Arial;font-size:12px;">'
        
        
        vtemp = (n+1)%2
        if  vtemp ==0:
            Format='<td style="background-color:#f7f7f7;font-family:Arial;font-size:12px;">'
            if df_temp.loc[n,'ZONA']=='TOTAL':
                Format='<td style="background-color:#DFDFDF;font-family:Arial;font-size:12px;font-weight: bold;">'
            x=x+Format+ df_temp.loc[n,'ZONA'] +'</TD>'
            
        else:
            
            x=x+Format+ df_temp.loc[n,'ZONA'] +'</TD>'
        
        
        x=x+Format+ str(df_temp.loc[n,'AVANCE'])+'</TD>'
        x=x+Format+ str(df_temp.loc[n,'META'])+'</TD>'
        
        x=x+Format+image(df_temp.loc[n,'ALCANCE'])+" " +str(df_temp.loc[n,'ALCANCE'])+"%"+'</TD>'
        

    if dot!='SUSANA':
        x=x+'<tr style="height:10px;"></tr>'
x=x + '</body></table></html>'
x=x + '<br><p Style = "font-family:Arial;font-weight: bold;font-size:12px">*Se considera Solicitudes con Meta y Tiendas Activas</p>'
x=x + '<br><br><p style="font-family:Arial;font-weight: bold;font-size:12px">Saludos'
x=x + '<br>Desarrollo de Negocios</p>'   
x=x + '<img src="https://i.imgur.com/gqh4ipx.png"/>'

x=Body_1+Cabecera+x
x=x.replace('ZONA ','')
#x=x.replace(' ','Total')




Destinatarios='ntocasc@intercorp.com.pe'
Copias ='''gtamaram@intercorp.com.pe;dcedanoo@intercorp.com.pe;ntocasc@intercorp.com.pe;vortegaa@intercorp.com.pe'''
#Destinatarios='lcardenas@intercorp.com.pe;lhuayanar@intercorp.com.pe;ocontena@intercorp.com.pe;'
#Copias = 'ycordova@intercorp.com.pe;tmancilla@intercorp.com.pe;ntocasc@intercorp.com.pe;vortegaa@intercorp.com.pe;acuzcoab@intercorp.com.pe;kesteban@intercorp.com.pe;gtamaram@intercorp.com.pe'
#Copias = 'ntocasc@intercorp.com.pe;vortegaa@intercorp.com.pe;acuzcoab@intercorp.com.pe;kesteban@intercorp.com.pe;gtamaram@intercorp.com.pe;dcedanoo@intercorp.com.pe'
Subject='Alerta Avance Solicitudes CTS  ' + periodo
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
    #cnxn = pyodbc.connect(r'Driver=SQL Server;Server=CFERNANDEZSAXP\SERVIDORRETAIL;Database=BDTCADQ;UID=usuario_dn;PWD=escritura;')
    #cursor_2 = cnxn.cursor()
    #cnxn.autocommit = True
    cursor_2.execute("exec [dbo].[GCVE_ALERTA_GENERICA_CC] ?,?,?,?", x, Subject , Destinatarios,Copias)
    
    print('Enviado')
except Exception: # as e:
    print('Error al conectarse al Fernandez')

print('Tiempo de Ejecucion: ' ,datetime.now() - start)