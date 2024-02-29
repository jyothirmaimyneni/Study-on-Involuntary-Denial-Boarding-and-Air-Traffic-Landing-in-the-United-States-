#!/usr/bin/env python
# coding: utf-8

# Importing the Data for Air Traffic Statistics

# In[57]:


import pandas as pd
fileobject = 'C:/Users/Sai_Gontyala/Downloads/Air_Traffic_Landings_Statistics.json'
f = open(fileobject)
data_json=json.load(f)
Air_Traffic_Landings=data_json["data"]
Air_Traffic_Landings


# Reading the json data to a dataframe

# In[58]:


Air_Traffic_Landings_data = pd.DataFrame(Air_Traffic_Landings, columns=['1','2','3','4','5','6','7','8','Activity_Period','Operating_Airline','Operating_Airline_IATA_Code','Published_Airline','Published_Airline_IATA_Code','GEO_Summary','GEO_Region','Landing_Aircraft_Type','Aircraft_Body_Type','Aircraft_Manufacturer','Aircraft_Model','Aircraft_Version','Landing_Count','Total_Landed_Weight'])
Air_Traffic_Landings_data=Air_Traffic_Landings_data.iloc[: , 8:]


# Deleting the existing collection

# In[59]:


import pymongo
d = collection.delete_many({})
print(d.deleted_count, " documents deleted.")


# Installing pymongo

# In[ ]:


pip install pymongo


# Creating database in mongo DB and loading data into a test collection

# In[60]:


import pymongo

from pymongo import MongoClient
client = MongoClient('localhost', 27017)

db = client.test_database
collection = db.test_collection
document=collection.insert_many(Air_Traffic_Landings_data.to_dict('r'))


# In[9]:


import pprint as pp
for Air_Traffic_Landings_data in db.test_collection.find():
    pp.pprint(Air_Traffic_Landings_data)


# In[77]:


Data_from_AirTraffic_ld = pd.DataFrame(list(collection.find()))
#Data_from_AirTraffic


# In[78]:


Df=Data_from_AirTraffic_ld= Data_from_AirTraffic_ld.drop(['_id'], axis=1) #removed the sytem genrated id column
#Df


# Data Preprocessing

# In[79]:


Airtraffic_ld_null_counts = Df.isnull().sum()
null_column_list=Airtraffic_ld_null_counts[Airtraffic_ld_null_counts > 0].sort_values(ascending=False)
null_column_list


# In[81]:


null_column_list=["Operating_Airline_IATA_Code","Published_Airline_IATA_Code","Aircraft_Version","Aircraft_Manufacturer"]

for column in null_column_list:
    Df[column].fillna("NA",inplace = True)



# In[82]:


# view null values
Df.isnull().sum().reset_index(name = "Null values").set_index("index")


# In[83]:


Df.dtypes


# In[84]:


print("No. of aircaft versions provided is {}.".format(Df["Aircraft_Version"].nunique()))


# In[85]:


Df["Aircraft_Version"].value_counts().rename_axis("Aircraft_Version").reset_index(name = "count").set_index("Aircraft_Version").head()


# In[89]:


# count of unique values in each column
print(Df.nunique())


# In[71]:


Df


# In[87]:


Df.drop(['Operating_Airline_IATA_Code', 'Operating_Airline_IATA_Code'],axis=1, inplace=True)


# In[88]:


Df.drop(['Published_Airline_IATA_Code'],axis=1, inplace=True)


# In[ ]:


pip install psycopg2


# In[ ]:


import pandas as pd
import json
from pymongo import MongoClient
import sqlalchemy
import psycopg2
engine = sqlalchemy.create_engine('postgresql://postgres:password@localhost:5432/postgres')


# In[ ]:


import psycopg2
try:
    dbConnection = psycopg2.connect(user = "dap",password = "dap",host = "192.168.56.30",port = "5432",database = "postgres")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute("CREATE DATABASE DAP;")
    print("Created database")
    dbCursor.close()
except (Exception , psycopg2.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(dbConnection): dbConnection.close()


# In[ ]:


createtable = """
CREATE TABLE Air_Traffic_Statistics(
Activity_Period         varchar(100),
Operating_Airline       varchar(100),
Published_Airline       varchar(100),
GEO_Summary             varchar(100),
GEO_Region              varchar(100),
Landing_Aircraft_Type   varchar(100),
Aircraft_Body_Type      varchar(100),
Aircraft_Manufacturer   varchar(100),
Aircraft_Model          varchar(100),
Aircraft_Version        varchar(100),
Landing_Count           varchar(100),
Total_Landed_Weight     varchar(100)   
);    
"""

try:
    dbConnection = psycopg2.connect(user = "dap",password = "dap",host = "192.168.56.30",port = "5432",database = "postgres")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute(createtable)
    dbCursor.close()

except (Exception , psycopg2.Error) as dbError :
    print ("PostgreSQL connection issue", dbError)
finally:
    if(dbConnection): dbConnection.close()


# In[ ]:


#Extracting data from Postgres
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
sql = """
SELECT * FROM Air_Traffic_Statistics;
"""
try:
    dbConnection = psycopg2.connect(user = "dap",password = "dap",host = "192.168.56.30",port = "5432",database = "postgres")
    Df = sqlio.read_sql_query(sql, dbConnection)
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(dbConnection): dbConnection.close()


# Data transformation

# In[91]:


Df["Activity_Year"]=(Df.Activity_Period.astype(str).str)[:4]


# In[90]:


# reset activity period to a datetime. 
Df["Activity_Period"] = pd.to_datetime(Df["Activity_Period"], format = "%Y%m")

# establish year variable
Df["Year"] =Df["Activity_Period"].dt.year

# print date range
print("This dataset covers the years from", Df["Year"].min(),"to {}.".format(Df["Year"].max()))


# In[92]:


Df["Activity_Month"]=(Df.Activity_Period.astype(str).str)[4:]


# In[25]:


Df


# In[26]:


numerical = [var for var in Df.columns if Df[var].dtype!='O'] #check numerical columns
categorical = [var for var in Df.columns if Df[var].dtype == 'O']


# Visualization

# In[27]:


from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt # plotting
import numpy as np # linear algebra
import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


# In[29]:


Df['Landing_Count']=Df['Landing_Count'].astype(int)
Df['Total_Landed_Weight']=Df['Total_Landed_Weight'].astype(int)


# In[56]:


import seaborn as sns
sns.catplot(x = "GEO_Summary", kind = "count", hue ="Landing_Aircraft_Type", data = Df, 
            palette = "YlGnBu", height = 6, aspect = 2,)

#modified graph
plt.title("GEO Summary for flight type");
#plt.annotate('Source: DataSF, 2022 - https://data.sfgov.org/Transportation/Air-Traffic-Landings-Statistics/fpux-q53t', (0,-.15), xycoords ='axes fraction' )

plt.show()


# In[42]:


import matplotlib.pyplot as plt
import seaborn as sns
df_1 = Df.copy()

#revise shape of dataset to enable resampling based on time
df_1P = df_1[df_1["Landing_Aircraft_Type"]== "Passenger"][["Activity_Period","Total_Landed_Weight"]]
df_1F = df_1[df_1["Landing_Aircraft_Type"]== "Freighter"][["Activity_Period","Total_Landed_Weight"]]

df_1P.set_index("Activity_Period", inplace = True)
df_1F.set_index("Activity_Period", inplace = True)


#create graph
plt.figure(figsize = (20,8))
df_1P["Total_Landed_Weight"].resample(rule="A").mean().plot.line(label = "Passenger", color =  "#177DAE", lw=2).legend(loc='upper right')

df_1F["Total_Landed_Weight"].resample(rule="A").mean().plot(label = "Freighter", color = "#58D68D",lw=2).legend(loc='upper right')


#modified graph
plt.title("Average yearly total landed weight")
sns.despine(top = True, right = True, left = False, bottom = False)
#plt.annotate('Source: DataSF, 2022 - https://data.sfgov.org/Transportation/Air-Traffic-Landings-Statistics/fpux-q53t', (0,-.1), xycoords ='axes fraction' )
plt.ylabel("Total Landed Weight (Million Tonnes)", fontsize=11)

plt.show()


# In[53]:


Freighter = Df[Df["Landing_Aircraft_Type"]=="Freighter"]
Freight_Boeing_Wide = Freighter[(Freighter["Aircraft_Manufacturer"]=="Boeing")& (Freighter["Aircraft_Body_Type"]=="Wide_Body")]


# In[45]:


plt.figure(figsize = (20,8))
sns.countplot(x = "GEO_Region", hue = "Aircraft_Body_Type", data = Df, palette = "YlGnBu_r", hue_order =["Narrow Body", "Wide Body", "Regional Jet", "Turbo Prop"])

#modified graph
plt.title("Distribution of aircraft body type for passenger flights by GEO Region")
plt.legend(loc='upper right')
sns.despine(top = True, right = True, left = False, bottom = False)
#plt.annotate('Source: DataSF, 2022 - https://data.sfgov.org/Transportation/Air-Traffic-Landings-Statistics/fpux-q53t', (0,-.1), xycoords ='axes fraction' )

plt.show()


# In[50]:


#create graph
plt.figure(figsize = (20,8))
sns.countplot(x = "GEO_Region", hue = "Aircraft_Body_Type", data = Df[Df["Landing_Aircraft_Type"]=="Freighter"], 
              palette = "YlGnBu_r", hue_order = ["Wide Body", "Narrow Body", "Regional Jet", "Turbo Prop"])

#modified graph
plt.title("Distribution of aircraft body type for cargo flights by GEO region")
plt.legend(loc='upper right')
sns.despine(top = True, right = True, left = False, bottom = False)
#plt.annotate('Source: DataSF, 2022 - https://data.sfgov.org/Transportation/Air-Traffic-Landings-Statistics/fpux-q53t', (0,-.1), xycoords ='axes fraction' )
plt.show()


# In[48]:


# establish new variables for narrow and wide body passenger planes
Passenger_Boeing_Wide = Df[(Df["Aircraft_Manufacturer"]=="Boeing") &(Df["Aircraft_Body_Type"]=="Wide Body")]
Passenger_Airbus_Narrow = Df[(Df["Aircraft_Manufacturer"]=="Airbus") &(Df["Aircraft_Body_Type"]=="Narrow Body")]

#create graph
plt.figure(figsize = (20,8))
sns.countplot(x = "Aircraft_Model", data = Passenger_Boeing_Wide, order =Passenger_Boeing_Wide["Aircraft_Model"].value_counts().index , palette = "YlGnBu_r")

#modified graph
plt.title("Aircraft model distribution for Boeing wide body passenger planes")
sns.despine(top = True, right = True, left = False, bottom = False)
#plt.annotate('Source: DataSF, 2022 - https://data.sfgov.org/Transportation/Air-Traffic-Landings-Statistics/fpux-q53t', (0,-.1), xycoords ='axes fraction' )

plt.show()

