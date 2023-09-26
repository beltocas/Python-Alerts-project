# -*- coding: utf-8 -*-
"""
Created on Sat Nov 12 09:39:04 2022

@author: B39670
"""

from datetime import datetime
start = datetime.now()
import smtplib
from email.mime.multipart import MIMEMultipart
#from email.mime.base import MIMEBase
from email.mime.text import MIMEText

import pyodbc
import pandas as pd
#import teradatasql
import sqlalchemy as db

def linking(server):
    if server=='FERNANDEZ':
        server_name = 'CFERNANDEZSAXP\SERVIDORRETAIL' 
        database_name = 'BDTCADQ' 
        driver = 'SQL Server' 
        username='usuario_dn'
        password='escritura'
        link = f'DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        #link = db.create_engine('mssql+pyodbc://'+username+':'+password+'@' + server_name + '/' + database_name+ '?driver='+driver) 
    elif server=='VALLADARES':
        server_name = 'FVALLADARESXP\SERVIDOR_RETAIL2' 
        database_name = 'fvalladaresxp' 
        driver = 'SQL Server' 
        username='Seguros'
        password='Lectura$1'
        link = f'DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        #link = db.create_engine('mssql+pyodbc://'+username+':'+password+'@' + server_name + '/' + database_name+ '?driver='+driver) 
    elif server=='COMISIONES':
        server_name = 'BP2620CTCP5HOW10' 
        database_name = 'DWH_CONCURSO' 
        driver = 'SQL Server' 
        username='usuario_lectura'
        password='usuario_lectura'
        link = f'DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        #link = db.create_engine('mssql+pyodbc://'+username+':'+password+'@' + server_name + '/' + database_name+ '?driver='+driver) 
    elif server=='TDT':
        hostname = '10.11.240.31'
        username='APP_PlanContrGestCanales'
        password='frBNeXosBa'
        driver='Teradata Database ODBC Driver 16.20'#'10.11.240.31'#
        link = 'DRIVER={driver};DBCNAME={hostname};UID={uid};PWD={pwd}'.format(
                      driver=driver,hostname=hostname,  
                      uid=username, pwd=password)   
    return link

def procedure_sql(query,server,tipo): 
    engine=linking(server)
    conection = pyodbc.connect(engine)
    if server!='TDT':
        try:
            if tipo == 'execute':
                engine.execute(query)
                print('Query ejecutada')
            else:
                df = pd.read_sql(query,conection)
                print('Dataframe importado')
                return df
        except Exception: # as e:
            print('Error al conectarse ' + server)
    else:
        if tipo == 'execute':
            cnxn = pyodbc.connect(conection)
            cursor_2 = cnxn.cursor()
            cnxn.autocommit = True
            cursor_2.execute(query)
        else:
            cnxn = pyodbc.connect(conection)
            cursor_2 = cnxn.cursor()
            cnxn.autocommit = True
            #cursor_2.execute(query)
            df = pd.read_sql(query,cnxn)
            print('Dataframe importado')
            return df 
        
# PERIODO='202211'
DICCIONARIO={'TNO': '{:.0%}','DESEMBOLSO': '{:,.0f}','MINIMO_OPERACIONES': '{:,.0f}','CROSS_SEGURO': '{:,.0f}','HORAS_LABORALES':'{:%Y/%m/%d %H:%M:%S}',
             'CROSS_PA': '{:,.0f}','CROSS_TARJETA_ADICIONAL': '{:,.0f}','CROSS_TOTAL': '{:,.0f}','TARJETA_ACTIVA': '{:,.0f}','TARJETA_APROBADA': '{:,.0f}'
             ,'MINIMO_TARJETA_ACTIVA': '{:,.0f}','GESTION_PERSONAS': '{:.0%}','GESTION_BASE': '{:.0%}','GESTION_CALIDAD': '{:.0%}','CALIDAD': '{:.0%}'
             ,'AGENDAMIENTO': '{:,.0f}','TIEMPO_CONEXION': '{:.0%}'
             ,'UPGRADE': '{:,.0f}','INCREMENTO_LINEA': '{:,.0f}','CARTERIZACION': '{:,.0f}'
             ,'MINIMO_VENTA': '{:,.0f}','TOTAL_VENTA': '{:,.0f}','FPD': '{:,.0f}'
             ,'RETENCION_ALTO_VALOR': '{:.0%}','RETENCIONES_TC': '{:,.0f}'
             ,'RETENCIONES_SEGUROS_Y_CTS': '{:,.0f}','VENTA': '{:,.0f}'
             ,'EFECTIVIDAD_RETENCION': '{:.0%}','ENCUESTAS': '{:,.0f}'
             ,'CROSS_MAVERICK': '{:,.0f}','RETENCIONES': '{:,.0f}'
             ,'SATISFACCION': '{:.0%}','TIEMPO_HABLADO(%)':'{:.0%}'
             ,'CANTIDAD_TRASLADO_CTS': '{:,.0f}','SABS': '{:,.0f}'
             ,'MONTO_TRASLADO_CTS': '{:,.0f}','APERTURAS': '{:,.0f}' #PAGO_NO_PRESENCIAL
             ,'ABONOS_CTS': '{:,.0f}','OPERACIONES': '{:,.0f}','IZIPAY': '{:,.0f}','PAGO_NO_PRESENCIAL': '{:,.0f}',
             'IZIPAY_PAGOS':'{:,.0f}'
             ,'DESEMBOLSO': '{:,.0f}','NPS': '{:.1%}','ACEPTA_CALIFICA': '{:,.0f}'
             }


QUERY=''' SELECT * FROM CORREO_METAS_GCV ORDER BY PUESTO,REGISTRO_SV,REGISTRO_EV,META,TIPO_META'''

df= procedure_sql(QUERY,'FERNANDEZ','select')

#df.head()

#if df['EQUIPO'] is not None:
EQUIPO=df.loc[0,'EQUIPO']
#else:
#    print("Objeto equipo fallo")

SUB_EQUIPO= 'FFVV CS Y CTS' if df.loc[0,'EQUIPO']=='FFVV CS Y CTS' else ('HIPOTECARIO' if df.loc[0,'EQUIPO']=='HIPOTECARIO' else df.loc[0,'SUB_EQUIPO'])
HORA =df.loc[0,'FECHA_ACTUALIZACION']
TIPO=df.loc[0,'TIPO_CARGA']
MES=df.loc[0,'PERIODO']

df['EJECUTIVO']=df['EJECUTIVO'].str[:29]

df_jf=df[df.PUESTO=='JEFE'].pivot_table(index=('REGISTRO_SV','REGISTRO_EV','EJECUTIVO'),columns='TIPO_META', values='META',fill_value=None)
df_sv=df[df.PUESTO=='SUPERVISOR'].pivot_table(index=('REGISTRO_SV','REGISTRO_EV','EJECUTIVO'),columns='TIPO_META', values='META',fill_value=None)
df_ev=df[(df.PUESTO=='EJECUTIVO')|(df.PUESTO=='ASESOR')].pivot_table(index=('SUB_EQUIPO','REGISTRO_SV','REGISTRO_EV','EJECUTIVO'),columns='TIPO_META', values='META',fill_value=None)





if 'HORAS_LABORABLES' in df_ev.columns:
    df_ev['HORAS_LAB']=df_ev['HORAS_LABORABLES'].apply(lambda x: str(int(x*24)) +":" +str( int((x*24)%1 *60)).ljust(2,'0') if x>0 else None)
    df_ev.drop('HORAS_LABORABLES',axis=1,inplace=True)
else:
    print('Hay horas laborales')


if 'TIEMPO_HABLADO' in df_ev.columns:
    df_ev['T_HABLADO']=df_ev['TIEMPO_HABLADO'].apply(lambda x: str(int(x*24)) +":" +str( int((x*24)%1 *60)).ljust(2,'0') if x>0 else None)
    df_ev.drop('TIEMPO_HABLADO',axis=1,inplace=True)
    print('OK')
else:
    print('No Hay tiempo hablado')

if 'HORAS_LABORABLES' in df_sv.columns:
    df_sv['HORAS_LAB']=df_sv['HORAS_LABORABLES'].apply(lambda x: str(int(x*24)) +":" +str( int((x*24)%1 *60)).ljust(2,'0') if x>0 else None)
    df_sv.drop('HORAS_LABORABLES',axis=1,inplace=True)
elif 'TIEMPO_LOGUEO' in df_sv.columns:
    df_sv['T_LOGUEO']=df_sv['TIEMPO_LOGUEO'].apply(lambda x: str(int(x*24)) +":" +str( int((x*24)%1 *60)).ljust(2,'0') if x>0 else None)
    df_sv.drop('TIEMPO_LOGUEO',axis=1,inplace=True)
else:
    print('No Hay logueo')
    
if 'TIEMPO_HABLADO' in df_sv.columns:
    df_sv['T_HABLADO']=df_sv['TIEMPO_HABLADO'].apply(lambda x: str(int(x*24)) +":" +str( int((x*24)%1 *60)).ljust(2,'0') if x>0 else None)
    df_sv.drop('TIEMPO_HABLADO',axis=1,inplace=True)
    print('OK')
else:
    print('No hay tiempo hablado')

df_jf.reset_index(inplace=True)    
df_ev.reset_index(inplace=True)    
df_sv.reset_index(inplace=True)

if SUB_EQUIPO=='HIPOTECARIO':
    df_sv=df_sv[df_sv['REGISTRO_SV'] != 'TOTAL']

#HTML CON ESTILO

styles = [
    dict(selector="tr:hover",
                props=[("background", "#D6EEEE")]),
    dict(selector="th.col_heading", props=[("color", "#fff"),
                               ("border", "0.5px solid #eee"),
                               ("font-family" , 'Arial'),
                               #("padding", "12px 35px"),
                               ("border-collapse", "collapse"),
                               ("background", "#1D4477"),             
                               ("font-size", "10px"),
                               ("width","50pt"),
                               ]),
    dict(selector="th.row_heading", props=[("color", "#fff"),
                               ("border", "1px solid #eee"),
                               #("padding", "12px 35px"),
                               ("border-collapse", "collapse"),
                               ("background", "yellow"),
                               ("font-size", "10px")
                               ]),
    dict(selector="td", props=[("font-family" , 'Arial'),
                               ("color", "#000000"),
                               ("border", "0.3px solid #eee"),
                               ("padding", "5px 5px"),
                               ("border-collapse", "collapse"),
                               ("font-size", "10px"),
                               ("text-align","center"),
                               ("width","50pt"),
                               ]),
    dict(selector="table", props=[                                   
                                    ("font-family" , 'Helvetica'),
                                    ("margin" , "auto"),
                                    ("border-collapse" , "collapse"),
                                    ("border" , "0.3px solid #eee"),
                                    ("border-bottom" , "1px solid #00cccc"),
                                    ("text-align","center"),
                                      ]),
    dict(selector="caption", props=[("caption-side", "bottom")]),
    dict(selector="tr:nth-child(even)", props=[
        ("background-color", "#f2f2f2"),
    ]),
]

TITULO='METAS ' + TIPO +": " +SUB_EQUIPO + "-" +MES

XY=f'''
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <div>
      <p style="text-align:center;font-family:Arial;font-size:10px;padding:5px 7px;border-bottom:2px #2F4F4F solid;font-weight:bold;Color:#2F4F4F"></p>
    </div>
    <p style="text-align:center;font-family:Arial;font-size:10px;padding:5px 7px;border-top:2px #0070C0 solid;border-bottom:2px #0070C0 solid;font-weight:bold;background-color:#1D4477;color:#ffffff">{TITULO}</p>
   '''



HTML_1=df_sv.style.format(DICCIONARIO)\
             .set_table_styles(styles)\
             .hide(axis='index')\
             .to_html(index=False)
             

HTML_2=df_ev.style.format(DICCIONARIO)\
             .set_table_styles(styles)\
             .hide(axis='index')\
             .to_html(index=False)

if len(df_jf)>0:
    HTML_0=df_jf.style.format(DICCIONARIO)\
                 .set_table_styles(styles)\
                 .hide(axis='index')\
                 .to_html(index=False)
    html='<p style="font-family:Arial;font-size:12px;font-weight: bold;color:black;">► Meta Jefes </p>' + HTML_0
else:
    html=''

html=html+'<br><p style="font-family:Arial;font-size:12px;font-weight: bold;color:black;">► Meta Supervisores </p>'

html=html + HTML_1

html=html+'<br><p style="font-family:Arial;font-size:12px;font-weight: bold;color:black;">► Meta Ejecutivos </p>'

html=html + HTML_2
html=html+'<p style="font-family:Arial;font-size:12px;color:black;">Hora de carga EC: ' + HORA + ' </p>'
html=html + '<p style="font-family:Arial;font-weight: bold;font-size:12px">Saludos'
html=html + '<br>Desarrollo de Negocios</p>'   
html=html + '<img src="https://i.imgur.com/gqh4ipx.png"/>'
html=html.replace('nan%','')      
html=html.replace('nan','')
html=html.replace('None','')
html=html.replace('TARJETA_','T_')
# html=html.replace('CROSS_','CRX_')
html=html.replace('MINIMO_','MIN_')
html=html.replace('GESTION_','GEST_')
html=html.replace('RETENCIONES_','RET_')
html=html.replace('RETENCION_ALTO_VALOR','ALTO_VALOR')
html=html.replace('TIEMPO_','T_')
html=html.replace('REGISTRO','REG')
html=html.replace('EJECUTIVO','NOMBRE_EJECUTIVO')
html=XY+html
""" msg = MIMEMultipart()
msg['From'] = 'mordoez@intercorp.com.pe'

# recipients = ['mordoez@intercorp.com.pe','jcollantesgo@intercorp.com.pe','jromo@intercorp.com.pe','ntocas@intercorp.com.pe','vortegaa@intercorp.com.pe']
msg['To'] = 'mordoez@intercorp.com.pe'
# # msg['To'] ="; ".join(recipients)
msg['Subject'] = 'METAS CARGADAS ' + MES+": " +SUB_EQUIPO + "-"+ TIPO
msg.attach(MIMEText(html, 'html'))
server = smtplib.SMTP('smtp.office365.com', 587)  ### put your relevant SMTP here
server.ehlo()
server.starttls()
server.ehlo()
server.login('mordoez@intercorp.com.pe', 'G@laxy24')  ### if applicable
server.send_message(msg)
server.quit() """

DESTINATARIOS='mdavilas@intercorp.com.pe;nquispein@intercorp.com.pe;gcardenasfri@intercorp.com.pe;wsoriaa@intercorp.com.pe;rmoncadap@intercorp.com.pe'
#dvillaverde@intercorp.com.pe;lprimo@intercorp.com.pe;abeniteso@intercorp.com.pe'#';vortegaa@intercorp.com.pe;jcollantesgo@intercorp.com.pe;jromo@intercorp.com.pe;ntocas@intercorp.com.pe'
#DESTINATARIOS='ntocasc@intercorp.com.pe;kesteban@intercorp.com.pe'
#COPIAS='vortegaa@intercorp.com.pe;ntocasc@intercorp.com.pe;gtamaram@intercorp.com.pe;acuzcoab@intercorp.com.pe'
COPIAS='tmancilla@intercorp.com.pe;vortegaa@intercorp.com.pe;ntocasc@intercorp.com.pe;dcedanoo@intercorp.com.pe;gtamaram@intercorp.com.pe;acuzcoab@intercorp.com.pe;kesteban@intercorp.com.pe' #'mordoez@intercorp.com.pe'


SUBJECT='METAS CARGADAS ' + MES+": " +SUB_EQUIPO + "-"+ TIPO

# print="exec [dbo].[GCVE_ALERTA_GENERICA_CC] ?,?,?,?", html, SUBJECT , DESTINATARIOS,COPIAS

# print(query)

try:
      cnxn = pyodbc.connect(r'Driver=SQL Server;Server=BP2620CTCP5HOW10;Database=DWH_CONCURSO;UID=usuario_lectura;PWD=usuario_lectura;')
      cursor_2 = cnxn.cursor()
      cnxn.autocommit = True
      cursor_2.execute("exec [dbo].[ALERTA_GENERICA] ?,?,?,?", html, SUBJECT , DESTINATARIOS,COPIAS )
      print('Enviado')
except Exception: # as e:
      print('Error al conectarse al Comisiones')

# procedure_sql(query,'COMISIONES','execute')
# 


print('Tiempo de Ejecucion: ' ,datetime.now() - start)