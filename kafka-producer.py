from kafka import KafkaProducer
import pandas as pd
import time
from json import dumps
import random
KAFKA_TOPIC_NAME_CONS = "orderstopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
   print("Kafka Producer Application Started.....")
   kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                      value_serializer=lambda x: dumps(x).encode('utf-8'))
   orders_Filepath = "/home/hadoop/Downloads/orders.csv"
   orders_pd_pf = pd.read_csv(orders_Filepath)

   print(orders_pd_pf.head(1))
   orders_list = orders_pd_pf.to_dict(orient="records")

   for message in orders_list:
       print("Message to be sent: ", message)
       kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
       time.sleep(1)

