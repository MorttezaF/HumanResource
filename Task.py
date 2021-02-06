from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pandas as pd
from pyspark.sql.types import *


# Read Datasets with Pandas

df1 = pd.read_csv('tiers.csv')

df2 = pd.read_csv('facilities.csv')

# Create Spark Session 

spark=SparkSession.builder.appName('join_morteza').getOrCreate()
sc= spark.sparkContext

# Create Spark Context

sqlCtx = SQLContext(sc)

# Convert tiers dataset to Spark DataFrame

re_t=df1['Region']
fa_t=df1['Facility']
ti_t=df1['Tier']
tiers=[]
for i in range(len(re_t)):
	tiers.append((re_t[i],fa_t[i],ti_t[i]))
dff1 = pd.DataFrame(tiers, columns=('Region','Facility','Tier'))

# tiers Spark DataFrame

df_sp1 = sqlCtx.createDataFrame(dff1.astype(str))

# Convert facilities dataset to Spark DataFrame

df_list=['Region','District','FacilityName','Type','Town','Ownership','Latitude','Longitude']

tiers=[]
list_d=[]
for i in df_list:
	list_d.append(df2[i])
for i in range(len(df2['Region'])):
	tiers.append((list_d[0][i],list_d[1][i],list_d[2][i],list_d[3][i],list_d[4][i],list_d[5][i],list_d[6][i],list_d[7][i]))

dff2 = pd.DataFrame(tiers, columns=('Region','District','FacilityName','Type','Town','Ownership','Latitude','Longitude'))



# facilities Spark DataFrame

df_sp2 = sqlCtx.createDataFrame(dff2.astype(str))


# assign alias name to datasets

ta = df_sp1.alias('ta')
tb = df_sp2.alias('tb')

# joins

join = ta.join(tb, ta.Region == tb.Region)
full_outer_join = ta.join(tb, ta.Region == tb.Region,how='full')
left_outer_join = ta.join(tb, ta.Region == tb.Region,how='left')
right_outer_join = ta.join(tb, ta.Region == tb.Region,how='right')

join=join.select(ta.Region,'District','FacilityName','Type','Town','Ownership','Latitude','Longitude','Facility','Tier')

# Store the resulting dataframe on disk
join.write.csv('new_dataset.csv')

#rectangular area by latitude and longitude

join.select("Latitude","Longitude").show()


