from datetime import datetime 
import os
import win32com.client
import pandas as pd
#import logging
import pyodbc
# import smtplib
# # from email.mime.multipart import MIMEMultipart
# # from email.mime.base import MIMEBase
# from email.mime.text import MIMEText


start = datetime.now()
char= '202309'
data = {'Reporte' : ['Comisiones Diarias - TLV TC', 'Comisiones Diarias - TLV PORTAFOLIO', 'Comisiones Diarias - RETENCIÓN'], 
        'celda' : ['A1','A1','A1'],
        'hoja' : ['Comisiones EV´s', 'Comisiones - EV GdP', 'R. Asesor'],
        'copiado':['J87:T87','K36:X36','J46:Z46'],
        'pegado':['J89:T89','K39:X39','J48:Z48'],
        'destino' : [r'C:\Users\B41319\Documents\Reportes TLV\Actualizaciones\TC',
                    r'C:\Users\B41319\Documents\Reportes TLV\Actualizaciones\GDP',
                    r'C:\Users\B41319\Documents\Reportes TLV\Actualizaciones\RETE']                     
                     } 

df = pd.DataFrame(data)
ruta=r'C:\Users\B41319\Documents\Reportes TLV\Maquetas'
#df=df.loc[[0]]
#df.reset_index(drop=True, inplace=True)

print('Estoy Iniciando')
xlapp = win32com.client.DispatchEx("Excel.Application")

# ACTUALIZACION REPORTES TLV PRODUCTOS, BPE B, BPE C

for i in range(len(df)): 
    archivo_inicial = df.loc[i,'Reporte'] + '.xlsb' 
    path=df.loc[i,'destino'] 
    #path=r'C:\Users\B39670\Documents\Avances\Anteriores'
    input = os.path.join(ruta, archivo_inicial) 
    # df.loc[i,'Fecha_Inicio']='01.01.2022'
    # df.loc[i,'Fecha_Fin']='01.01.2022'
    try:
        wb = xlapp.Workbooks.Open(input)
        xlapp.DisplayAlerts = False
        dato_i = wb.Worksheets(df.loc[i,'hoja']).Range(df.loc[i,'celda'])
        # num_i = wb.Worksheets(df.loc[i,'hoja']).Range('A2')
        print('-------------------')
        print('Fecha Inicio: '+str(dato_i))
        df.loc[i,'Fecha_Inicio']=str(dato_i)
        # df.loc[i,'Reales_Inicio']=float(str(num_i))
        # ws=wb.Worksheets("Comisiones EV´s")
        wb.Worksheets(df.loc[i,'hoja']).Range(df.loc[i,'copiado']).Copy()
        wb.Worksheets(df.loc[i,'hoja']).Range(df.loc[i,'pegado']).PasteSpecial(Paste=-4163)
        wb.RefreshAll() 
        xlapp.CalculateUntilAsyncQueriesDone()
        dato_f = wb.Worksheets(df.loc[i,'hoja']).Range(df.loc[i,'celda'])
        # num_f = wb.Worksheets(df.loc[i,'hoja']).Range('A2')
        #df.loc[i,'Fecha_Corte']=dato_f
        archivo_final = df.loc[i,'Reporte'] +' - '+ str(dato_f) +'.xlsb'
        output = os.path.join(path,archivo_final) 
        #output = os.path.join(path,char,archivo_final) 
        wb.Save()
        print(df.loc[i,'Reporte'], sep='\n')
        print('Fecha Fin: '+str(dato_f))
        df.loc[i,'Fecha_Fin']=str(dato_f)
        # print(int(df.loc[i,'Fecha_Fin'][0:1]))
        # wb.SaveAs(Filename:= output) 
        # df.loc[i,'Reales_Fin']=float(str(num_f))
        if int(df.loc[i,'Fecha_Fin'][0:2])>=int(df.loc[i,'Fecha_Inicio'][0:2]):
            wb.SaveAs(Filename:= output)  
            #df.loc[i,'Estado']="Actualizó"
        elif int(df.loc[i,'Fecha_Fin'][0:2])<int(df.loc[i,'Fecha_Inicio'][0:2]):
            df.loc[i,'Fecha_Fin']=str(dato_f)
            # df.loc[i,'Reales_Fin']=float(str(num_f))
            # df.loc[i,'Estado']="Reales errados"
            print("--> Reales errados")
        else:
            # df.loc[i,'Estado']="No vario"
            print("--> No vario")
        # wb.SaveAs(Filename:= output) 
    except Exception:# as error:
        #logging.error(error)
        #print(df.loc[i,'nombre'],  dato_i, sep='\n')
        df.loc[i,'Fecha_Fin']='No actualizó'
        # df.loc[i,'Reales_Fin']=float(0)
        # df.loc[i,'Estado']="Error"
        print('--> Hubo error')
#del(wb)    
xlapp.Quit()
print('-------------------')

Body='''<!DOCTYPE html><html><body>
        Equipo,
        <br>Alcanzamos el estatus de la actualización de reportes.<br>
        <br>Enlace:
        &nbsp;<a href="file://bp2620ctcp5how10/Users/usrretail/Documents/Actualizar/">Carpeta</a>
        <br><br>  
        <table style="width: 80%; border-collapse: collapse; margin-left: auto; margin-right: auto;text-align: center;" border="0">
          <tbody>
            <th style="width: 7.5%; height:18px;background-color:darkgreen;color:white;border-radius:0px;font-weight: bold;" ;" colspan="4">Reportes TLV - TC - GDP</th>
            <tr style="height:18px;">
              <th style="width: 7.5%; height:18px;background-color:#00235d;color:white">Reporte</th>
              <th style="width: 7.5%; height:18px;background-color:#00235d;color:white">Inicio</th>
              <th style="width: 7.5%; height:18px;background-color:#00235d;color:white">Fin</th>
              <th style="width: 7.5%; height:18px;background-color:#00235d;color:white">Enlace</th>
              </tr>'''

x=''
Format='<td style="width:7.5%;height:18px;">'
df=df.iloc[:,[0,6,7]]
for i in range(len(df)):
    x=x+'<tr style="height:18px;background-color:#f2f2f2;border-radius:20px;">'
    x=x+Format+df.loc[i,'Reporte']+'</TD>'
    x=x+Format+df.loc[i,'Fecha_Inicio']+'</TD>'
    x=x+Format+df.loc[i,'Fecha_Fin']+'</TD>'
    if i==0:
        y='Enlace:&nbsp;<a href='+'https://interbankpe-my.sharepoint.com/:f:/r/personal/reportesretail_intercorp_com_pe/Documents/1.%20Reportes/05.%20Team%20Performance%20FFVV/4.%20Televentas/1.%20Comisiones/1.%20TLV%20TC,%20Retenci%C3%B3n,%20GDP/1.Comisiones%20Diarias%20-%20Detalle%20Jefe%20-%20TC?csf=1&web=1&e=1aW6P5'+'>TC</a></TD>'
    elif i==1:
        y='Enlace:&nbsp;<a href='+'https://interbankpe-my.sharepoint.com/:f:/r/personal/reportesretail_intercorp_com_pe/Documents/1.%20Reportes/05.%20Team%20Performance%20FFVV/4.%20Televentas/1.%20Comisiones/1.%20TLV%20TC,%20Retenci%C3%B3n,%20GDP/3.%20Comisiones%20Diarias%20-%20Detalle%20Supervisor%20-%20Portafolio?csf=1&web=1&e=HOqk6f'+'>GDP</a></TD>'
    elif i==2:
        y='Enlace:&nbsp;<a href='+'https://interbankpe-my.sharepoint.com/:f:/r/personal/reportesretail_intercorp_com_pe/Documents/1.%20Reportes/05.%20Team%20Performance%20FFVV/4.%20Televentas/1.%20Comisiones/1.%20TLV%20TC,%20Retenci%C3%B3n,%20GDP/4.%20Comisiones%20Diarias%20-%20Detalle%20Supervisor%20-%20Retenci%C3%B3n?csf=1&web=1&e=31fy1o'+'>Retención</a></TD>'
    else:
        y=''
    x=x+Format+y
    #'Enlace:&nbsp;<a href="file://bp2620ctcp5how10/Users/usrretail/Documents/Actualizar/">Carpeta</a>'+'</TD>'


x=x+'</body>'
x=x+'</table>'
x=x+'</html>'
x=Body+x
x=x.replace('Comisiones Diarias - ','')
# x=x.replace('202207','202206') 
x=x + '''
    <br><br>
    <p style="font-family:Arial;font-weight: bold;font-size:12px">Saludos
    <br>Desarrollo de Negocios</p> 
    <img src="https://i.imgur.com/gqh4ipx.png"/>
    '''   

print("Correo en curso")

# msg = MIMEMultipart()
# msg['From'] = 'mordoez@intercorp.com.pe'
# #msg['From'] = 'martinordonezr@outlook.com'
# msg['To'] = 'mordoez@intercorp.com.pe'
# msg['Subject'] = 'Alerta TC Retención ' + 'Julio 2022'

# msg.attach(MIMEText(x, 'html'))
# server = smtplib.SMTP('smtp.office365.com', 587)  ### put your relevant SMTP here
# server.ehlo()
# server.starttls()
# server.ehlo()
# server.login('mordoez@intercorp.com.pe', 'G@laxy23')  ### if applicable
# server.send_message(msg)
# server.quit()

Destinatarios='ntocasc@intercorp.com.pe;vortegaa@intercorp.com.pe;dcedanoo@intercorp.com.pe;gtamaram@intercorp.com.pe' 

try:
    cnxn = pyodbc.connect(r'Driver=SQL Server;Server=CFERNANDEZSAXP\SERVIDORRETAIL;Database=BDTCADQ;UID=usuario_dn;PWD=escritura;')
    cursor_2 = cnxn.cursor()
    cnxn.autocommit = True
    cursor_2.execute("exec [dbo].[GCVE_ALERTA_GENERICA] ?,?,?", x, 'ALERTA AVANCES TLV TC-GDP' , Destinatarios )
except Exception: # as e:
    print('Error al conectarse al Fernandez')



print('Tiempo de Ejecucion: ' ,datetime.now() - start)
