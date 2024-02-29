#!/usr/bin/env python
# coding: utf-8

# Importing the Data for Air Traffic

# In[34]:


# import urllib library
from urllib.request import urlopen
  
# import json
import json
# store the URL in url as 
# parameter for urlopen
url = "https://data.sfgov.org/api/views/rkru-6vcg/rows.json?accessType=DOWNLOAD"
  
# store the response of URL
response = urlopen(url)
  
# storing the JSON response 
# from url in data
data_json = json.loads(response.read())
  
# print the json response
#print(data_json)


# Reading the json data to a dataframe

# In[35]:


import pandas as pd
Air_Traffic_Passenger=data_json["data"]
Air_Traffic_Passenger_data = pd.DataFrame(Air_Traffic_Passenger, columns=['1','2','3','4','5','6','7','8','Activity_Period','Operating_Airline','Operating_Airline_IATA_Code','Published_Airline','Published_Airline_IATA_Code','GEO_Summary','GEO_Region','Activity_Type_Code','Price_Category_Code','Terminal','Boarding_Area','Passenger_Count'])
Air_Traffic_Passenger_data=Air_Traffic_Passenger_data.iloc[: , 8:]
#Air_Traffic_Passenger_data


# Deleting the existing collection

# In[36]:


import pymongo
d = collection.delete_many({})
print(d.deleted_count, " documents deleted.")


# Installing pymongo

# In[ ]:


pip install pymongo


# Creating database in mongo DB and loading data into a test collection

# In[37]:


import pymongo

from pymongo import MongoClient
client = MongoClient('localhost', 27017)

db = client.test_database
collection = db.test_collection
document=collection.insert_many(Air_Traffic_Passenger_data.to_dict('r'))


# In[9]:


import pprint as pp
for Air_Traffic_Passenger_data in db.test_collection.find():
    pp.pprint(Air_Traffic_Passenger_data)


# In[38]:


Data_from_AirTraffic = pd.DataFrame(list(collection.find()))


# In[39]:


Df=Data_from_AirTraffic= Data_from_AirTraffic.drop(['_id'], axis=1) #removed the sytem genrated id column
#Df


# Data Preprocessing

# In[40]:


Airtraffic_null_counts = Df.isnull().sum()
null_column_list=Airtraffic_null_counts[Airtraffic_null_counts > 0].sort_values(ascending=False)
null_column_list


# In[41]:


null_column_list=["Operating_Airline_IATA_Code","Published_Airline_IATA_Code"]

for column in null_column_list:
    Df[column].fillna("NA",inplace = True)



# In[42]:


Df.dtypes


# In[43]:


# count of unique values in each column
print(Df.nunique())


# In[44]:


Df


# In[50]:


Df.dtypes


# In[45]:


Df.drop(['Operating_Airline_IATA_Code', 'Operating_Airline_IATA_Code'],axis=1, inplace=True)


# In[46]:


Df.drop(['Published_Airline_IATA_Code'],axis=1, inplace=True)


# In[47]:


Df.drop(['Boarding_Area'],axis=1, inplace=True)


# In[48]:


Df.drop(['Published_Airline'],axis=1, inplace=True)


# In[49]:


Df


# In[ ]:


pip install psycopg2


# In[53]:


import pandas as pd
import json
from pymongo import MongoClient
import sqlalchemy
import psycopg2
engine = sqlalchemy.create_engine('postgresql://postgres:password@localhost:5432/postgres')


# In[55]:


import psycopg2
try:
    dbConnection = psycopg2.connect(user = "jyothi",password = "honey",host = "192.168.0.78",port = "5433",database = "postgres")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute("CREATE DATABASE DAP;")
    print("Created database")
    dbCursor.close()
except (Exception , psycopg2.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(dbConnection): dbConnection.close()


# In[54]:


import psycopg2
try:
    dbConnection = psycopg2.connect(host = "192.168.0.78",port = "5432",database = "postgres")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute("CREATE DATABASE DAP;")
    print("Created database")
    dbCursor.close()
except (Exception , psycopg2.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(dbConnection): dbConnection.close()


# In[51]:


createtable = """
CREATE TABLE AirTraffic_data(
Activity_Period varchar(10),
Operating_Airline varchar(50),
Published_Airline varchar(50),
GEO_Summary varchar(250),
GEO_Region varchar(20),
Activity_Type_Code varchar(20),
Price_Category_Code varchar(20),
Terminal varchar(10),
Passenger_Count number(18)
);
"""

try:
    dbConnection = psycopg2.connect(user = "jyothi",password = "honey",host = "192.168.0.78",port = "5432",database = "postgres")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute(createtable)
    dbCursor.close()

except (Exception , psycopg2.Error) as dbError :
    print ("PostgreSQL connection issue", dbError)
finally:
    if(dbConnection): dbConnection.close()


# In[33]:


#Extracting data from Postgres
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
sql = """
SELECT * FROM AirTraffic_data;
"""
try:
    dbConnection = psycopg2.connect(user = "jyothi",password = "honey",host = "192.168.0.78",port = "5432",database = "postgres")
    Df = sqlio.read_sql_query(sql, dbConnection)
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(dbConnection): dbConnection.close()


# Data transformation

# In[25]:


Df["Activity_Year"]=(Df.Activity_Period.astype(str).str)[:4]


# In[26]:


Df["Activity_Month"]=(Df.Activity_Period.astype(str).str)[4:]


# In[27]:


Df['Qtr'] = pd.to_datetime(Df['Activity_Month'].values, format='%m').astype('period[Q]')
Df["Qtr"]=(Df.Qtr.astype(str).str)[4:]


# In[28]:


Df


# In[20]:


numerical = [var for var in Df.columns if Df[var].dtype!='O'] #check numerical columns
categorical = [var for var in Df.columns if Df[var].dtype == 'O']


# Visualization

# In[30]:


from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt # plotting
import numpy as np # linear algebra
import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


# In[21]:


Df['Passenger_Count']=Df['Passenger_Count'].astype(int)


# In[32]:


grouped_by_airline = Df.groupby("Operating_Airline").agg\
({ 
    "Operating_Airline" : "count",
    "Passenger_Count" : lambda x : np.mean(x), #mean passengers count by airlines

})

grouped_by_airline.rename(columns = {"Operating_Airline" : "nb_flights", 
                                   "Passenger_Count" : "mean_passenger_count"}, 
                          inplace = True)

grouped_by_airline = grouped_by_airline.sort_values(by = "nb_flights", ascending = False)

grouped_by_airline=grouped_by_airline.head(10).round()

grouped_by_airline.plot(kind="bar",
                      grid=False,
                      figsize=(16,10),
                      #color="r",
                      alpha = 0.5,
                      width=0.6,
                      stacked = False,
                     edgecolor="g",)


# In[33]:


import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

df_sorted=pd.DataFrame(Df['Passenger_Count'].sort_values(ascending=False))[:5]
df_sorted['Qtr']=Df['Qtr']
Df.groupby('Qtr').sum()['Passenger_Count']
f2=plt.pie(x=df_sorted['Passenger_Count'],explode=[0.10]*5,labels=df_sorted['Qtr'],autopct='%1.2f%%',shadow=True)
plt.xlabel('Qtr')
plt.ylabel('')
plt.title('Top 5 genres that requires high budget')
plt.tight_layout()



# In[28]:


Df['Activity_Year']=Df['Activity_Year'].astype(int)


# In[29]:


Df['Activity_Month']=Df['Activity_Month'].astype(int)


# In[30]:


Df['Activity_Period']=Df['Activity_Period'].astype(int)


# In[37]:


import seaborn as sns
f,ax = plt.subplots(figsize=(5, 5))
sns.heatmap(Df.corr(), annot=True, linewidths=0.5,linecolor="red", fmt= '.1f',ax=ax)
plt.show()


# In[38]:


Df.corr()


# In[23]:


Df


# In[39]:


GEO_Region = Df.groupby('GEO_Region')['Passenger_Count'].count()
GEO_Region_df = pd.DataFrame({'code':GEO_Region.index, 'Passenger_Count':GEO_Region.values})
GEO_Region_df


# In[40]:


import plotly.graph_objects as go

fig = go.Figure(data=go.Choropleth(
    locations = GEO_Region_df['code'], # Spatial coordinates
    z = GEO_Region_df['Passenger_Count'].astype(float), # Data to be color-coded
    locationmode = 'country names', # set of locations match entries in `locations`
    colorscale = 'Greens',
    colorbar_title = "Count Bar",
))

fig.update_layout(
    title_text = 'Most travelled countries by passengers',
    geo_scope='world', # limite map scope to USA
)

fig.show()


# In[ ]:


['ISO-3', 'USA-states', 'country names', 'geojson-id']
  'africa', 'asia', 'europe', 'north america', 'south
          america', 'usa', 'world'


# In[42]:


import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

year=Df.groupby('Activity_Year').sum()['Passenger_Count']
print(year.tail())
Df.groupby('Activity_Year').sum()['Passenger_Count'].plot(xticks=np.arange(1960,2016,5))

sns.set(rc={'figure.figsize':(10,5)})
plt.title("Passenger Count per year",fontsize=14)
plt.xlabel('Year',fontsize=12)
sns.set_style("whitegrid")


# In[43]:


import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

df_sorted=pd.DataFrame(Df['Passenger_Count'].sort_values(ascending=False))[:5]
df_sorted['Qtr']=Df['Qtr']
Df.groupby('Qtr').sum()['Passenger_Count']
f2=plt.pie(x=df_sorted['Passenger_Count'],explode=[0.10]*5,labels=df_sorted['Qtr'],autopct='%1.2f%%',shadow=True)
plt.xlabel('Qtr')
plt.ylabel('Passenger_Count')
plt.title('Top 5 genres that requires high budget')
plt.tight_layout()



# In[32]:


import matplotlib.pyplot as plt
import seaborn as sns
PAX_month_yr = Df.groupby(["Activity_Year","Activity_Month"])["Passenger_Count"].sum().divide(1000).round()
PAX_month_yr = PAX_month_yr.reset_index()

pivot_2 = PAX_month_yr.pivot_table(values="Passenger_Count",index="Activity_Month",columns="Activity_Year", fill_value=0)
pivot_2.index=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

sns.set(font_scale=0.8)
fig = plt.figure(figsize=(12,8))
g = sns.heatmap(pivot_2, annot=True, linewidths=.5, fmt="d", square =True, vmin=2000, cmap=sns.cm.rocket_r)
g.set_title("Number of passengers in each month (in thousands)", fontweight="bold")
g.set_yticklabels(g.get_yticklabels(), rotation=0)
plt.tight_layout()
plt.show()

