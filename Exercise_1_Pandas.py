# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 12:58:30 2023

@author: arnab
"""
import pandas as pd
import mysql.connector
import pymongo
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:mysql@localhost/arnab123')

#Creating Mongodb Connection
client = pymongo.MongoClient("mongodb+srv://arnab:automation1@cluster0.rvlks0z.mongodb.net/?retryWrites=true&w=majority")
mymongodb = client.test
database = client['Dress']
collection = database["Dress Statistics"]

# 1. Create a  table attribute dataset and dress dataset
# 2. Do a bulk load for these two table for respective dataset 
df_Attribute = pd.read_excel(r"C:\Users\arnab\Documents\DATA SCIENCE\DATASETS\Attribute DataSet.xlsx")
df_DressSales = pd.read_excel(r"C:\Users\arnab\Documents\DATA SCIENCE\DATASETS\Dress Sales.xlsx").fillna(0)
df_DressSales = df_DressSales.replace(r'[a-zA-Z]*|removed', 0)
df_DressSales['Total Sales'] = df_DressSales.sum(axis = 1)

# Create the connection and close it(whether successed of failed)
# with engine.begin() as connection:
#   df_Attribute.to_sql(name='Attribute', con=connection, if_exists='append', index=False)
#   df_DressSales.to_sql(name='Dress Sales', con=connection, if_exists='append', index=False)

# 3. read these dataset in pandas as a dataframe
with engine.connect() as con:
    # df_Attribute_sql = pd.DataFrame(con.execute('SELECT * FROM arnab123.`attribute`;'))
    # df_DressSales_sql = pd.DataFrame(con.execute('SELECT * FROM arnab123.`dress sales`;'))
    df_Attribute_sql = pd.DataFrame(con.execute('SELECT * FROM arnab123.`attribute`;'), columns = con.execute('SELECT * FROM arnab123.`attribute`;')._metadata.keys)


# 4. Convert attribute dataset in json format 
#df_Attribute_sql['json'] = df_Attribute_sql.to_json(orient='records', lines=True).splitlines()


# 5. Store this dataset into mongodb
#collection.insert_many(df_Attribute_sql['json'])
# collection.insert_many(df_Attribute_sql.to_dict('records'))

# 6. in sql task try to perform left join operation with attrubute dataset and dress dataset on column Dress_ID

with engine.connect() as con:
    df_left_join = pd.DataFrame(con.execute('SELECT * FROM arnab123.`attribute` as attribute LEFT JOIN arnab123.`dress sales` as dress_sales USING(Dress_ID);'), columns = con.execute('SELECT * FROM arnab123.`attribute` as attribute LEFT JOIN arnab123.`dress sales` as dress_sales USING(Dress_ID);')._metadata.keys)
# 7. Write a sql query to find out how many unique dress that we have based on dress id    
    unique_Dress_Count = con.execute('SELECT COUNT( Dress_ID )FROM arnab123.`dress sales`;').fetchall()[0][0]
    
# 8. Try to find out how many dress is having recommendation 0
    zero_Recommendation = con.execute('SELECT COUNT( Dress_ID ) FROM arnab123.`attribute` WHERE Recommendation = 0;').fetchall()[0][0]
    
# 9. Try to find out total dress sell for individual dress id 
df_Total_Sales = df_left_join[['Dress_ID', 'Total Sales']]
df_Total_Sales = df_Total_Sales.sort_values('Total Sales')
# 10. Try to find out a third highest most selling dress id
third_Highest_Sales = df_Total_Sales.take([2]).Dress_ID.values[0]

# columnNames = []
# with engine.connect() as con:
#     count = 0
#     for i in con.execute('SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "dress sales" ORDER BY ORDINAL_POSITION;').fetchall():
        
#         if count > 0 and count < 24:
#             columnNames.append(i[0])
#         count+=1