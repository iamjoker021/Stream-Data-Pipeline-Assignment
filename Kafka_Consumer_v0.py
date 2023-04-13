#!/usr/bin/env python
# coding: utf-8

# In[16]:


import time
import json
import kafka

import pandas as pd
import numpy as np
import datetime

# For DashBoard
import streamlit as st

# import plotting libraries
import seaborn as sns
import matplotlib.pyplot as plt

# In[17]:


bootstrap_server = "localhost:9092"
consumer = kafka.KafkaConsumer(
    bootstrap_servers=[bootstrap_server],
    key_deserializer=lambda key: key.decode('utf-8'),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


# In[18]:


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



from collections import deque
  
# Initializing a queue
q = deque()
q.extend([None, None, None, None, None])

# In[ ]:
placeholder = st.empty()


for message in consumer:
    
    print(f"k={message.key}, v={message.value}")
    
    # creating rolling avg
    roll_avg = np.nan
    if len(df) >= 5:
        roll_avg = df["price"].iloc[-5:].mean()
        
    # Adding record to DataFrame
    timestamp = datetime.datetime.strptime(message.value[0], "%Y-%m-%d %H:%M:%S")
    price = message.value[1]
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
        st.pyplot(fig)
        
        ## Section 3
        st.markdown("### Detailed Data View")
        st.dataframe(df.iloc[-5:])
