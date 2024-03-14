# Databricks notebook source
import pyodbc
import psycopg2
 
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=AZEUND0008,40031;'
                      'Database=ICQM;'
                      )
 
cursor = conn.cursor()
cursor.execute("select * from fca400.[Audit Table];")
row = cursor.fetchone()
 
print(row)
print(type(row))

# COMMAND ----------

print('Test')


# COMMAND ----------


