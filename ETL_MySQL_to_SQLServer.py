import pandas as pd
import mysql.connector as connection
import pyodbc

#Accesos MySQL (origen)
host="localhost"
database ='difoca'
user="root"
passwd="root1"

#Accesos a SQLServer (destino)
host1="10.200.9.11"
database1 ='db_sysdifoca'
user1="user_sysdifoca"
passwd1="user_sysdifoca"


#funcion insert_new_values
def sql_merge(tabla, llave):
    #traer tabla MySQL (origen)
    try:
        con = connection.connect(host=host, database =database,user=user, passwd=passwd,use_pure=True)
        query = "select * from "+tabla
        dfx = pd.read_sql(query,con)
        con.close() 
    except Exception as e:
        con.close()
        print(str(e))
        
    #traer tabla SQLServer (destino)
    try:
        con = pyodbc.connect('DRIVER={SQL Server};SERVER='+host1+';DATABASE='+database1+';UID='+user1+';PWD='+passwd1)
        query = "select * from "+tabla
        dfy = pd.read_sql(query,con)
        con.close()
    except Exception as e:
        con.close()
        print(str(e))
    
    #buscar nuevos valores
    df = pd.concat([dfy, dfx]) 
    df = df.reset_index(drop=True)
    df["cant"]=1
   
    df1=df.groupby(llave)["cant"].count()
    df2 = pd.DataFrame(df1)
    df2.reset_index(inplace=True)

    df3 = df.merge(df2, on=llave, how="left")
    df4=df3[df3["cant_y"]==1]
    df4.pop("cant_x")
    df4.pop("cant_y")
    df4 = df4.astype (str)
    
    df4.reset_index(inplace=True)
    df4.pop("index")
    cols = ",".join([str(i) for i in df4.columns.tolist()])
    con = pyodbc.connect('DRIVER={SQL Server};SERVER='+host1+';DATABASE='+database1+';UID='+user1+';PWD='+passwd1)
    cur=con.cursor()
    for (index,rs) in df4.iterrows():
        vals = "','".join([str(xi) for xi in df4.values.tolist()[index]])
        sql = "INSERT INTO "+tabla+" (" +cols + ") VALUES ('" + vals + "')"
        cur.execute(sql)
    con.commit()
    con.close()
    

#Aplicar funcion (tabla_sql, columna_llave)

sql_merge("curso_prueba","dni")