from kafka import KafkaConsumer
import json

import datetime as dt
import pandas as pd
from utils import query_func, insert_func

bootstrap_servers='b-2.mskcluster1.bb3wpy.c3.kafka.ap-southeast-1.amazonaws.com:9092'
topicName = 'rejected'

consumer = KafkaConsumer(topicName,bootstrap_servers = bootstrap_servers)

def customer_func(DbConfig,m):
    m = eval(m)
    date_i = m['Application Date']
    
    ## calculation MA50
    sql = """with df_date as (select  
        r."Application Date" as date_
        , avg(r."Risk_Score") as v
    from postgres.public.rejected r 
    where  r."Application Date" <= '{}'
    and r."Application Date" > '{}'
    group by r."Application Date"
    order by date_
    )
    select *
    ,round(ema(cast(df_date.v as INTEGER), 0.5 ) over (order by df_date.date_ asc),2) AS "MA50"
    ,AVG(df_date.v)
           OVER(ORDER BY df_date.date_ ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
           AS "MA100"        
    from df_date;
    """.format(
        date_i
        , (dt.datetime.strptime(date_i,'%Y-%m-%d') - dt.timedelta(days=100)).strftime('%Y-%m-%d')
    )
    data = query_func(DbConfig,sql)
    
    if len(data['data']) < 1:
        MA50 = m['Risk_Score']
        MA100 = m['Risk_Score']
    else:
        MA50 = round(data['data'][-1][2],2)
        MA100 = round(data['data'][-1][3],2)
    m['MA50'] = MA50
    m['MA100'] = MA100
    
    data_push = pd.DataFrame([m])
    insert_func(DbConfig,data_push,'postgres.public.rejected')
    return

DbConfig = json.load(open('config.json'))

for msg in consumer:
    data = msg.value.decode('utf-8')
    customer_func(DbConfig,data)
    print(data)