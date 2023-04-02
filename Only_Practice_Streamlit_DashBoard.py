## Streamlit

# !pip install streamlit

import streamlit as st
import pandas as pd


# import the libraries
import seaborn as sns
import matplotlib.pyplot as plt

import yfinance as yf
import datetime

df = yf.download(tickers='UBER', period='5d', interval='1m')

stock_dict = df["High"].to_dict().items()

schema = {
    "time": [],
    "price": [],
    "moving_avg": []
}
df = pd.DataFrame(schema)

st.set_page_config(
    page_title = 'Real-Time Data Science Dashboard',
    layout = 'wide'
)
st.title("Real-Time / Live Data Science Dashboard")

placeholder = st.empty()


import time
for d in stock_dict:
#while True: 
    
    price = d[1]

    # creating rolling avg
    roll_avg = None
    if len(df) >= 5:
        roll_avg = df.iloc[-5:, -2].mean()
        
    df.loc[len(df.index)] = [d[0], d[1], roll_avg]

    with placeholder.container():
        # create three columns
        kpi1, kpi2 = st.columns(2)

        # fill in those three columns with respective metrics or KPIs 
#         kpi1.metric(label="Price: ", value=price, delta= price - df["price"].get(-2))
        kpi2.metric(label="Price: ", value=round(price, 2))
        kpi2.metric(label="Rolling_Avg", value= round(roll_avg if roll_avg else 0, 2))
        
        st.markdown("Stock Graph with Moving average")
            
        fig = plt.figure(figsize=(9,6))
        # Time series plot with Seaborn lineplot()
        plt.plot(df["time"], df["price"], 'k.-', label='Original data')
        plt.plot(df["time"], df["moving_avg"], 'r.-', label='Running average')
        # axis labels
        plt.xlabel("Date", size=14)
#         plt.ylabel("Price", size=14)
        # save image as PNG file
        plt.savefig("Stock_Price_Chart.png",
                            format='png',
                            dpi=150)
        st.pyplot(fig)
        
        st.markdown("Stock Graph with Moving average")
        
        st.markdown("### Detailed Data View")
        st.dataframe(df.iloc[-5:])
#         time.sleep(1)