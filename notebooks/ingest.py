#!/usr/bin/env python
# coding: utf-8

# In[67]:


import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os


# In[10]:


print(os.listdir("../data/raw")[0])


# In[12]:


path = os.path.join("../data" , "raw", "yellow_tripdata_2024-08.parquet")
df = pd.read_parquet(path)


# In[44]:


df.head()


# In[45]:


df.columns


# In[14]:


df.isnull().sum()


# In[15]:


df = df.dropna(subset=[
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "payment_type"
])


# In[21]:


df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])


# In[22]:


df["trip_duration_min"] = (df["tpep_dropoff_datetime"] - 
                           df["tpep_pickup_datetime"]).dt.total_seconds() / 60


# In[24]:


df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
df["pickup_day"] = df["tpep_pickup_datetime"].dt.day_name()


# In[36]:


df = df[
    (df["trip_duration_min"] > 0) & 
    (df["trip_duration_min"] < 1440)
]


# In[37]:


df["trip_speed_kmh"] = df["trip_distance"] / (df["trip_duration_min"] / 60)


# In[40]:


df = df[
    (df["trip_distance"] > 0) &
    (df["trip_speed_kmh"] < 120)
]


# In[42]:


df["fare_per_km"] = df["fare_amount"] / df["trip_distance"]


# In[43]:


df.head()


# In[55]:


df = df[
    [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_hour",
        "pickup_day",
        "trip_distance",
        "trip_duration_min",
        "trip_speed_kmh",
        "fare_amount",
        "fare_per_km",
        "passenger_count",
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount', 
        'improvement_surcharge',
        'total_amount', 
        'congestion_surcharge'
    ]
]


# In[56]:


df.head()


# In[57]:


# Basic statistics
print(df.describe())


# In[ ]:


df["pickup_day"].value_counts()


# In[61]:


print(df["pickup_hour"].value_counts())


# In[68]:


# Trips per hour 
plt.figure(figsize=(10,5))
sns.countplot(x="pickup_hour" , data=df)
plt.title("Number of Trips per Hour")
plt.show()


# In[69]:


# Trips per day of week
plt.figure(figsize=(10,5))
sns.countplot(x="pickup_day" , data=df , order=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])
plt.title("Number of Trips Per Day of Week")
plt.show()


# In[76]:


df.head()


# In[80]:


duration_by_hour = df.groupby("pickup_hour")["trip_duration_min"].mean()
duration_by_hour


# In[82]:


distance_by_hour = df.groupby("pickup_hour")["trip_distance"].mean()  
distance_by_hour  


# In[87]:


# High Speed Trips

high_speed_trips = df[df["trip_speed_kmh"] > 110]
high_speed_trips.shape


# In[93]:


expensive_trips = df[df["total_amount"] > 250]
expensive_trips.shape

