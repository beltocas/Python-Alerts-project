
from datetime import datetime
import pandas as pd
import sqlalchemy as db
import numpy as np
import pyodbc

#Description of mail
descripton='''
    <p>
    Hola team, se reporta la actualizacion por dia - Canal TLV Tarjetas:
    </p>
    '''+ '''<p>Tarjetas aprobadas: '''+'''1000'''+'''</p>
    <p>Tarjetas activas: '''+'''20000'''+''' </p>'''

#Head of table in HTMLs
head= '''       
        <table width="520" style=" border-collapse: collapse; margin-left: auto; margin-right: auto;text-align: center;" border="0">
            <thead>
                <tr>
                    <th style=" padding: 7px; font-family:Arial;font-size:12px;background-color:#05334D;color:white" colspan="2" scope='colgroup'>TC Aprobadas</th>
                    <th style="padding: 7px; font-family:Arial;font-size:12px;background-color:#05BE50;color:white" colspan="2" scope='colgroup'>TC Activas</th>
                </tr>
                <tr>
                    <th style=" font-family:Arial;font-size:12px;border-style: groove;" scope='col'>Fecha</th>
                    <th style=" font-family:Arial;font-size:12px;border-style: groove;" scope='col'>Cantidad</th>
                    <th style=" font-family:Arial;font-size:12px;border-style: groove;" scope='col'>Fecha</th>
                    <th style=" font-family:Arial;font-size:12px;border-style: groove;" scope='col'>Cantidad</th>
                </tr>
                <!--<tr style="height:10px;"></tr>-->
            '''


#Body of html
body_table =''' '''+'''</table>'''
body = body_table

footer='''
    <br><br>
    <p style="font-family:Arial;font-weight: bold;font-size:12px">Saludos
    <br>Desarrollo de Negocios</p> 
    <img src="https://i.imgur.com/gqh4ipx.png"/>
    '''

x=descripton+head+body+footer



#Mandamos el mail mediante SP
Destinatarios='mpimentel@intercorp.com.pe;ntocasc@intercorp.com.pe;gtamaram@intercorp.com.pe'
Copias ='''gtamaram@intercorp.com.pe;'''
Subject='Informacion TLV - TC -  '
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