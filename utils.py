import pandas as pd
import numpy as np
import datetime as dt
import psycopg2
import json

def connectDb_func(DbConfig):
    conn = psycopg2.connect(**DbConfig)
    conn.autocommit = True
    return conn

def insert_func(DbConfig,data,tableName):
    data = data.fillna('NULL')
    sql = '''INSERT INTO {} ({}) VALUES {};'''.format(
        tableName
        , ",".join(['"{}"'.format(c) for c in data.columns])
        , ','.join(['('+",".join(["'{}'".format(v)  if type(v)==str else '{}'.format(v) for v in row])+')' for row in data.values])
    )
    conn = connectDb_func(DbConfig)
    cursor = conn.cursor()
    
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    return

def query_func(DbConfig,sql):
    conn = connectDb_func(DbConfig)
    cursor = conn.cursor()
    
    cursor.execute(sql)
    data = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    data = {
        'data' : data
        , 'columns' : colnames
    }
    
    cursor.close()
    conn.close()
    return data