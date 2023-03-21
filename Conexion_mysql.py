import mysql.connector as connection
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

server = {"h":"localhost","d":'difoca',"u":"root","p":"root1"}

def sql_df (tabla):
    con = connection.connect(host=server["h"], database =server["d"],user=server["u"], passwd=server["p"],use_pure=True)
    query = "select * from " + tabla
    df = pd.read_sql(query,con)
    con.close()
    return df

def query (query):
    con = connection.connect(host=server["h"], database =server["d"],user=server["u"], passwd=server["p"],use_pure=True)
    cur=con.cursor()
    cur.execute(query)
    con.commit()
    con.close()

def procedure1 (proc, list):
    con = connection.connect(host=server["h"], database =server["d"],user=server["u"], passwd=server["p"],use_pure=True)
    cur=con.cursor()
    cur.callproc(proc, list)
    con.commit()
    cur.close()
    con.close()
    
def procedure2 (proc, list_variables,list_columnas):
    con = connection.connect(host=server["h"], database =server["d"],user=server["u"], passwd=server["p"],use_pure=True)
    cur=con.cursor()
    cur.callproc(proc, list_variables)
    for i in cur.stored_results():
        results = i.fetchall()
    df = pd.DataFrame(results, columns=list_columnas)
    con.commit()
    cur.close()
    con.close()
    return df