from kafka import KafkaProducer
import json
import time

import pandas as pd

bootstrap_servers='b-2.mskcluster1.bb3wpy.c3.kafka.ap-southeast-1.amazonaws.com:9092'
topicName = 'rejected'

prod = KafkaProducer(bootstrap_servers = bootstrap_servers)

dict_dtype = {
    'Amount Requested' : 'float64'
    , 'Application Date' : 'str'
    , 'Loan Title' : 'str'
    , 'Risk_Score' : 'float64'
    , 'Debt-To-Income Ratio' : 'str'
    , 'Zip Code' : 'str'
    , 'State' : 'str'
    , 'Employment Length' : 'str'
    , 'Policy Code' : 'float64'
}

inter_df = pd.read_csv('rejected2009.csv',dtype = dict_dtype, iterator=True, chunksize=1)

for df in inter_df:
    m = df.iloc[0].fillna('NULL').to_dict()
    prod.send(topicName,json.dumps(m).encode('utf-8'))
    time.sleep(0.01)