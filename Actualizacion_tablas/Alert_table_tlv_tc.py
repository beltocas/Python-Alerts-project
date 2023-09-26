from datetime import datetime
import pandas as pd
import sqlalchemy as db
import numpy as np
import pyodbc



def insert_dates(date_arr,df_original,df_dates,column1,column2):
    #Loop that insert data
    for days in date_arr:
        if days not in df_dates:
            #Define the variables for the new row
            value_column1 = days
            value_column2 = 0
            #Variable of your insert in dataframe
            new_row = {column1: value_column1,column2: value_column2}
            #Use the append method to add the new row to the DataFrame
            df_original = df_original.append(new_row, ignore_index=True)
    return df_original

#Insert of array dates
def dates_array():
    import datetime
    current_date = datetime.date.today() +datetime.timedelta(days=-1)
    fisrt_date = current_date.replace(day=1)

    #initialize array of dates
    dates = []

    #fisrt insert in dates
    dates.append(fisrt_date)

    while fisrt_date < current_date:
        # calculate the next day
        fisrt_date += datetime.timedelta(days=1)
        #insert the next date 
        dates.append(fisrt_date)
    return dates


#function that insert in to body
def srch(df_list):
        #variables of scope function
        final=''
        b=0
        c=0
        x=0
        in_jump = '''<tr>'''
        jump_out = '''</tr>'''
        #print your array in html
        while x < len(df_list)*4:
            if b ==4:
                b=0
                c+=1
                final = final+jump_out
            if b==0:
                final = final+in_jump
            data_col = '''<td style ="border-style: groove;">'''+df_list[c][b]+'''</td>'''
            temp = data_col
            final +=data_col
            b+=1
            x+=1
        final = final+jump_out
        return final

if __name__ == "__main__":
    #Control execution time
    start = datetime.now()

    #Conection with your data base
    conn = pyodbc.connect('Driver={Teradata Database ODBC Driver 16.20};'
                        'DBCNAME={10.11.240.31};'
                        'UID={APP_PlanContrGestCanales};'
                        'PWD={frBNeXosBa}')

    period = '202309'

    #---------------------------------------------------
    #-- Generate your temporal table in teradata
    #---------------------------------------------------

    consulta = conn.cursor()
    #Build your temporal table 
    consulta.execute('''
    CREATE VOLATILE MULTISET TABLE TEMP_TC_HISTORICO_APROBADA_ALERT AS
        (
            SELECT 	Supervisor_Cd  REG_SUPERVISOR,Supervisor_Nm SUPERVIDOR,Promotor_Cd CODPROMOT,Promotor_Nm EJECUTIVO,documento_Num NRODOCTIT,Cliente_Cd,
                    CAST(AprobacionExpedADQ_Fc AS DATE) FECRESOL,MesAprobacionExpedADQ_Fc MESRESOL,CAST(EntregaTc_Fc AS DATE) FECENTREGA ,MesEntregaTc_Dsc MESENTREGA,MESACTIVACOMISION,
                    FECACTIVACOMISION,EquipoVenta_Dsc EQUIPO,SubEquipoVenta_Dsc SUBEQUIPO,EstadoSolicitud_Cd CODESTADO,Operacion_Cd CODOPERAC,Ooperacion_Dsc OPERACION
            --SELECT *
            FROM 	DLAB_DESNEGRET.CGR_TC_HISTORICO
            WHERE 	MesAprobacionExpedADQ_Fc>=(year(date)*100+month(date)-2) AND EstadoSolicitud_Cd=3 AND Operacion_Cd IN (1,2) --and NRODOCTIT = '75489595'
            AND 	Promotor_Cd IN (SELECT REG_EJECUTIVO FROM DLAB_DESNEGRET.MAESTRO_DOTACION WHERE PERIODO=(year(date)*100+month(date)) AND EQUIPO='TLV TARJETAS' 
                                AND sub_equipo = 'EMPRENDEDOR + PREFERENTE' AND PUESTO='EJECUTIVO' )
        )

    WITH DATA 
    PRIMARY INDEX(EJECUTIVO,NRODOCTIT ) ON COMMIT PRESERVE ROWS;
        ''')
    
    consulta.commit()
    consulta.close()
    
    

    consul_activa = conn.cursor()
    consul_activa.execute('''
    CREATE VOLATILE MULTISET TABLE TEMP_TC_HISTORICO_ACTIVAS_ALERT AS
        (
            SELECT 	Supervisor_Cd  REG_SUPERVISOR,Supervisor_Nm SUPERVIDOR,Promotor_Cd CODPROMOT,Promotor_Nm EJECUTIVO,documento_Num NRODOCTIT,Cliente_Cd,
                    AprobacionExpedADQ_Fc FECRESOL,MesAprobacionExpedADQ_Fc MESRESOL,EntregaTc_Fc FECENTREGA,MesEntregaTc_Dsc MESENTREGA,MESACTIVACOMISION,
                    FECACTIVACOMISION,EquipoVenta_Dsc EQUIPO,SubEquipoVenta_Dsc SUBEQUIPO,EstadoSolicitud_Cd CODESTADO,Operacion_Cd CODOPERAC,Ooperacion_Dsc OPERACION
            FROM 	DLAB_DESNEGRET.CGR_TC_HISTORICO
            WHERE 	MESACTIVACOMISION=(year(date)*100+month(date)) AND EstadoSolicitud_Cd=3 AND Operacion_Cd IN (1,2)
            AND 	Promotor_Cd IN (SELECT REG_EJECUTIVO FROM DLAB_DESNEGRET.MAESTRO_DOTACION WHERE PERIODO=(year(date)*100+month(date)) AND EQUIPO='TLV TARJETAS' 
                                AND sub_equipo = 'EMPRENDEDOR + PREFERENTE' AND PUESTO='EJECUTIVO' )
            --ORDER BY FECACTIVACOMISION DESC
        )
    WITH DATA 
    PRIMARY INDEX(EJECUTIVO,NRODOCTIT ) ON COMMIT PRESERVE ROWS;
    ''')
    consul_activa.commit()
    consul_activa.close()

    #Query in Teradata
    query_tmp_aprobada = '''
    SELECT FECRESOL, COUNT(*) CANTIDAD from TEMP_TC_HISTORICO_APROBADA_ALERT
    WHERE MESRESOL=(year(date)*100+month(date))
    AND CODESTADO=3 AND CODOPERAC IN (1,2)
    GROUP BY FECRESOL
    ORDER BY FECRESOL DESC;
    '''
    query_tmp_activa = '''
    SELECT FECACTIVACOMISION,COUNT(*) CANTIDAD FROM TEMP_TC_HISTORICO_ACTIVAS_ALERT
    WHERE MESACTIVACOMISION=(year(date)*100+month(date)) AND CODESTADO=3 AND CODOPERAC IN (1,2)
    GROUP BY 1
    ORDER BY FECACTIVACOMISION DESC;
    '''

    #Create dataframe from to querys
    data_avance_aprobada = pd.read_sql_query(query_tmp_aprobada,conn)
    data_avance_activa = pd.read_sql_query(query_tmp_activa,conn)

    #variables for total pointers
    sum_aprobada = data_avance_aprobada['CANTIDAD'].sum()
    sum_activa = data_avance_activa['CANTIDAD'].sum()
    
    #----------------------------------
    #Transform data
    #---------------------------------

    #dates of month
    date_arr = dates_array()
        
    #Parameters for insert data
    df_dates_aprobadas = data_avance_aprobada['FECRESOL'].to_numpy().tolist()
    column1 = data_avance_aprobada.columns[0]
    column2 = data_avance_aprobada.columns[1]

    df_dates_activas = data_avance_activa['FECACTIVACOMISION'].to_numpy().tolist()
    column_activa1 = data_avance_activa.columns[0]
    column_activa2 = data_avance_activa.columns[1]


    #Insert new rows
    df_new_aprobadas = insert_dates(date_arr,data_avance_aprobada,df_dates_aprobadas,column1,column2)

    df_new_activas = insert_dates(date_arr,data_avance_activa,df_dates_activas,column_activa1,column_activa2)


    #order your data for date
    df_new_aprobadas = df_new_aprobadas.sort_values(by='FECRESOL',ascending=False)
    df_new_activas = df_new_activas.sort_values(by='FECACTIVACOMISION',ascending=False)
    
    #Convert object to string data type
    df_new_aprobadas['FECRESOL'] = df_new_aprobadas['FECRESOL'].astype(str)
    df_new_aprobadas['CANTIDAD'] = df_new_aprobadas['CANTIDAD'].astype(str)

    df_new_activas['FECACTIVACOMISION'] = df_new_activas['FECACTIVACOMISION'].astype(str)
    df_new_activas['CANTIDAD'] = df_new_activas['CANTIDAD'].astype(str)

    
    #Convert the dataframe to numpy
    df_list_aprobadas = df_new_aprobadas.to_numpy().tolist()
    df_list_activas = df_new_activas.to_numpy().tolist()

    # Combina los elementos de array_1 y array_2 utilizando list comprehensions
    array_final = [item1 + item2 for item1, item2 in zip(df_list_aprobadas, df_list_activas)]

    #Call your function teacher
    tmp_table = srch(array_final)

    #---------------------------------------------------------------------------------------------------------------------

    #Description of mail
    descripton='''
    <p>
    Hola team, se reporta la actualizacion por dia - Canal TLV Tarjetas:
    </p>
    '''+ '''<p>Tarjetas aprobadas: '''+str(sum_aprobada) +'''</p>
    <p>Tarjetas activas: '''+str(sum_activa)+''' </p>'''

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
    body_table =''' '''+ tmp_table+'''</table>'''
   
    body = body_table

    footer='''
    <br><br>
    <p style="font-family:Arial;font-weight: bold;font-size:12px">Saludos
    <br>Desarrollo de Negocios</p> 
    <img src="https://i.imgur.com/gqh4ipx.png"/>
    '''

    x=descripton+head+body+footer



    #Mandamos el mail mediante SP
    Destinatarios='jcollantesgo@intercorp.com.pe;ntocasc@intercorp.com.pe'
    Copias ='''gtamaram@intercorp.com.pe;dcedanoo@intercorp.com.pe;vortegaa@intercorp.com.pe;kesteban@intercorp.com.pe;acuzcoab@intercorp.com.pe'''
    Subject='Informacion TLV - TC -  ' + period
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