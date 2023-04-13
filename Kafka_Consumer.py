#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import datetime
import json
import kafka
import streamlit as st # web development

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import io
import avro.schema
import avro.io


# In[17]:


# Without Schema
# bootstrap_server = "localhost:9092"
# consumer = kafka.KafkaConsumer(
#     bootstrap_servers=[bootstrap_server],
#     key_deserializer=lambda key: key.decode('utf-8'),
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )


# In[6]:


# With Schema
SCHEMA_PATH = "stock_schema.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())


# In[7]:


# With Schema
bootstrap_server = "localhost:9092"
consumer = kafka.KafkaConsumer(
    bootstrap_servers=[bootstrap_server]
)


# In[8]:


topic = 'hello'
consumer.subscribe(topics=[topic])
consumer.subscription()


# In[ ]:


schema = {
    "time": [],
    "price": [],
    "moving_avg": []
}
df = pd.DataFrame(schema)


# In[6]:


# class M:
#     def __init__(self, v):
#         self.key = "UBER"
#         self.value = v


# In[7]:


# consumer = [M(v) for v in range(10)]


# In[22]:


## Sample Rolling average calc
# from collections import deque
  
# # Initializing a queue
# q = deque()
# q.extend([None, None, None, None, None])

# for message in consumer:
#     q.append(message.value[1])
#     if len(q) > 5:
#         q.popleft()
#     roll_avg = None
#     if None not in q:
#         roll_avg = sum(q)/len(q)
#     print (f"k={message.key} time={message.value[0]} v={message.value[1]} roll_avg={roll_avg}")


# In[ ]:

# placeholder = st.empty()
# for message in consumer:
    
#     print(f"k={message.key}, v={message.value}")
    
#     # creating rolling avg
#     roll_avg = np.nan
#     if len(df) >= 5:
#         roll_avg = df["price"].iloc[-5:].mean()
        
#     # Adding record to DataFrame
#     timestamp = datetime.datetime.strptime(message.value[0], "%Y-%m-%d %H:%M:%S")
#     price = message.value[1]
#     df.loc[len(df.index)] = [timestamp, price, roll_avg]
    
#     with placeholder.container():
        
#         ## Section 1
#         # create two columns for Proce and Rolling Avg
#         kpi1, kpi2 = st.columns(2)

#         # fill in those three columns with respective metrics or KPIs
#         kpi2.metric(label="Price: ", value=round(price, 2), delta=round(price - df["price"].get(len(df)-2, 0), 4))
#         kpi2.metric(label="Rolling_Avg", value= round(roll_avg, 2))
        
#         ## Section 2
#         st.markdown("Stock Graph with Moving average")
            
#         fig = plt.figure(figsize=(9,6))
#         # Time series plot with Seaborn lineplot()
#         plt.plot(df["time"], df["price"], 'k.-', label='Original data')
#         plt.plot(df["time"], df["moving_avg"], 'r.-', label='Running average')
#         # axis labels
#         plt.xlabel("Date", size=14)
#         plt.ylabel("Price", size=14)
#         # save image as PNG file
#         plt.savefig("Stock_Price_Chart.png",
#                             format='png',
#                             dpi=150)
#         st.pyplot(fig)
        
#         ## Section 3
#         st.markdown("### Detailed Data View")
#         st.dataframe(df.iloc[-5:])


# In[ ]:

placeholder = st.empty()
for message in consumer:
    
    bytes_reader = io.BytesIO(message.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(SCHEMA)
    stock_data = reader.read(decoder)
    print(stock_data)
    
    # creating rolling avg
    roll_avg = np.nan
    if len(df) >= 5:
        roll_avg = df["price"].iloc[-5:].mean()
        
    # Adding record to DataFrame
    timestamp = datetime.datetime.strptime(stock_data["time"], "%Y-%m-%d %H:%M:%S")
    price = stock_data["price"]
    df.loc[len(df.index)] = [timestamp, price, roll_avg]
    
    with placeholder.container():
        
        ## Section 1
        # create two columns for Proce and Rolling Avg
        kpi1, kpi2 = st.columns(2)

        # fill in those three columns with respective metrics or KPIs
        kpi2.metric(label="Price: ", value=round(price, 2), delta=round(price - df["price"].get(len(df)-2, 0), 4))
        kpi2.metric(label="Rolling_Avg", value= round(roll_avg, 2))
        
        ## Section 2
        st.markdown("Stock Graph with Moving average")
            
        fig = plt.figure(figsize=(9,6))
        # Time series plot with Seaborn lineplot()
        plt.plot(df["time"], df["price"], 'k.-', label='Original data')
        plt.plot(df["time"], df["moving_avg"], 'r.-', label='Running average')
        # axis labels
        plt.xlabel("Date", size=14)
        plt.ylabel("Price", size=14)
        # save image as PNG file
        plt.savefig("Stock_Price_Chart.png",
                            format='png',
                            dpi=150)
        st.pyplot(fig)
        
        ## Section 3
        st.markdown("### Detailed Data View")
        st.dataframe(df.iloc[-5:])


# In[ ]:





# In[9]:


# # With Schema
# for message in consumer:
#     bytes_reader = io.BytesIO(message.value)
#     decoder = avro.io.BinaryDecoder(bytes_reader)
#     reader = avro.io.DatumReader(SCHEMA)
#     stock_data = reader.read(decoder)
#     print(stock_data, bytes_reader)
    

