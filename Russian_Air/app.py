# https://quera.org/problemset/211020   پروازهای روسیه

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db

import matplotlib.pyplot as plt
import seaborn as sns

import streamlit as st

plt.style.use("ggplot")

user = 'postgres'
password = '1q2w3e4r5'
host = '127.0.0.1'
port = 5432
database = 'russian_air'

url="postgresql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database)

engine = db.create_engine(url,pool_pre_ping=True)
conn = engine.connect() 
metadata = db.MetaData() 

aircrafts_data = db.Table('aircrafts_data', metadata,autoload_with=engine ,extend_existing=True)
airports_data = db.Table('airports_data', metadata,metadata,autoload_with=engine ,extend_existing=True)
boarding_passes = db.Table('boarding_passes', metadata,metadata,autoload_with=engine ,extend_existing=True)
flights = db.Table('flights', metadata,metadata,autoload_with=engine ,extend_existing=True)
seats = db.Table('seats', metadata,metadata,autoload_with=engine ,extend_existing=True)
ticket_flights = db.Table('ticket_flights', metadata,metadata,autoload_with=engine ,extend_existing=True)
tickets = db.Table('tickets', metadata,metadata,autoload_with=engine ,extend_existing=True)
metadata.create_all(engine) 


for t in metadata.sorted_tables:
    print(t.name)


# 1: Number of seats per plane by class
st.write("1: Number of seats per plane by class")

query = seats.select()
output = conn.execute(query).fetchall()
df1 = pd.DataFrame(output)

df1 = df1.groupby(["aircraft_code","fare_conditions"]).count().reset_index().sort_values(by=["aircraft_code","seat_no"],ascending=[True,False])\
    .rename(columns={"seat_no": "count" })

# st.dataframe(df1)
st.bar_chart(data = df1 , x = "aircraft_code",y="count" ,color="fare_conditions")\

# 2 : Total number of canceled flights on different days of the week
st.write("2 : Total number of canceled flights on different days of the week")

query = flights.select().where(flights.columns.status == 'Cancelled')
output = conn.execute(query).fetchall()
df2 = pd.DataFrame(output)
df2.scheduled_departure = pd.to_datetime(df2.scheduled_departure).dt.day_name()


df2 = df2.groupby("scheduled_departure").count()["status"].to_frame().reset_index().sort_values(by=["status","scheduled_departure"],ascending=[False,True])\
    .rename(columns={"scheduled_departure": "day","status": "count"}).reset_index(drop=True)
# st.dataframe(df2)
st.bar_chart(data= df2, x="count", y="day" )


# 3: Ranking of the number of passengers transported at airports by different aircraft
st.write("3: Ranking of the number of passengers transported at airports by different aircraft")

query1 = db.select(flights.columns.flight_id, 
                  flights.columns.departure_airport, 
                  flights.columns.aircraft_code).where(flights.columns.status != 'Cancelled')
output1 = conn.execute(query1).fetchall()

query2 = db.select(flights.columns.flight_id, 
                  flights.columns.arrival_airport, 
                  flights.columns.aircraft_code).where(flights.columns.status != 'Cancelled')
output2 = conn.execute(query2).fetchall()
df3 = pd.concat([pd.DataFrame(output1).rename(columns={"departure_airport": "airport_code"}),
                 pd.DataFrame(output2).rename(columns={"arrival_airport": "airport_code"})])


query3 = db.select(ticket_flights.columns.ticket_no,ticket_flights.columns.flight_id)
output3 = conn.execute(query3).fetchall()
df_t1 = pd.DataFrame(output3)

df3["count"] = df3["flight_id"].map(df_t1.flight_id.value_counts())
df3.dropna(inplace=True)
df3 = df3.drop("flight_id",axis=1)
df3.reset_index(drop=True,inplace=True)

df3 = df3.groupby(["airport_code","aircraft_code"]).sum("count").reset_index()

query4 = db.select(airports_data.columns.airport_code,airports_data.columns.airport_name)
output4 = conn.execute(query4).fetchall()
df_airport_name = pd.DataFrame(output4)

query5 = db.select(aircrafts_data.columns.aircraft_code,aircrafts_data.columns.model)
output5 = conn.execute(query5).fetchall()
df_aircraft_model = pd.DataFrame(output5)


df3["aircraft_model"] = df3["aircraft_code"].map(df_aircraft_model.set_index("aircraft_code").squeeze())
df3["airport"] = df3["airport_code"].map(df_airport_name.set_index("airport_code").squeeze().apply(lambda x: x['en']))
df3 = df3.drop(columns=["airport_code","aircraft_code"])
df3 = df3.sort_values(["airport","count"],ascending=[True,False]).reset_index(drop=True)


df_t = df3.groupby("airport")["count"].apply(lambda x: x.rank(method = "dense",ascending = False)).to_frame().reset_index(drop=True)
df3["rank"] = df_t

df3 = df3.reindex(columns=["airport","aircraft_model","count","rank"])

# st.dataframe(df3)
st.bar_chart(data=df3, x= "airport",y="count",color= "aircraft_model")
st.scatter_chart(data=df3, x= "airport",y="aircraft_model",color="count")

# 4: Time difference between different cities
st.write("4: Time difference between different cities")

query6 = db.select(flights.columns.scheduled_departure,
                   flights.columns.scheduled_arrival,
                   flights.columns.departure_airport,
                   flights.columns.arrival_airport)
output6 = conn.execute(query6).fetchall()
df4 = pd.DataFrame(output6)


df4.scheduled_departure = pd.to_datetime(df4.scheduled_departure)
df4.scheduled_arrival = pd.to_datetime(df4.scheduled_arrival)
df4["time_diffrrence"] = (df4.scheduled_arrival - df4.scheduled_departure).dt.total_seconds() /60
df4 = df4.drop(columns=["scheduled_arrival","scheduled_departure"])

df4 = df4.groupby(["departure_airport","arrival_airport"]).mean("time_diffrrence").reset_index()

query7 = db.select(airports_data.columns.airport_code,airports_data.columns.city)
output7 = conn.execute(query7).fetchall()
df_city = pd.DataFrame(output7)

df4["city1"] = df4.departure_airport.map(df_city.set_index("airport_code").squeeze()).apply(lambda x: x['en'])
df4["city2"] = df4.arrival_airport.map(df_city.set_index("airport_code").squeeze()).apply(lambda x: x['en'])
df4 = df4.drop(columns=["departure_airport","arrival_airport"])

df4['pair'] = df4.apply(lambda x: tuple(sorted([x["city1"], x["city2"]])), axis=1)
df4 = df4.drop(columns=["city1","city2"])

df4 = df4.groupby("pair")["time_diffrrence"].mean().to_frame().reset_index().sort_values("time_diffrrence", ascending=False)
df4.time_diffrrence = df4.time_diffrrence.round(1)
df4[["city1","city2"]] = df4["pair"].apply(pd.Series)
df4 = df4.drop(columns=["pair"]).reindex(columns=["city1","city2","time_diffrrence"]).reset_index(drop=True)



plt.figure(figsize=(15,12))
sns.heatmap(data= df4.set_index(["city1","city2"]).unstack(),
    xticklabels=df4.set_index(["city1","city2"]).unstack().columns.get_level_values(1),
    cmap=sns.color_palette("Blues", as_cmap=True),
    cbar_kws={"orientation": "horizontal"});

st.pyplot(plt)