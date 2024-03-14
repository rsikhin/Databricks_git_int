# Databricks notebook source
# MAGIC %md # Common_Utilities
# MAGIC
# MAGIC Version History of Common_Utilities
# MAGIC    
# MAGIC    Changes:
# MAGIC    
# MAGIC      Developer: Amar 
# MAGIC      Date Created: 02/01/2021
# MAGIC      Date Updated : 03/01/2021
# MAGIC      Purpose: Utility UDFs for CRPY and FSBCH
# MAGIC      

# COMMAND ----------

# DBTITLE 1,Convert Binary to Int
from pyspark.sql.types import IntegerType

binary_to_int = udf(lambda x: int(x, 2), IntegerType())

# Register the UDF
BinaryToInt = spark.udf.register("BinaryToInt", binary_to_int)

# COMMAND ----------

# DBTITLE 1,Send Email
# def send_email(sender_email, receiver_emails, subject, body, bcc=None, attachments=None):
#     import email, smtplib, ssl
#     from email import encoders
#     from email.mime.base import MIMEBase
#     from email.mime.multipart import MIMEMultipart
#     from email.mime.text import MIMEText  
#     # Create a multipart message and set headers
#     message = MIMEMultipart()
#     message["From"] = sender_email  
#     message["To"] = ','.join(receiver_emails) if isinstance(receiver_emails, list) else receiver_emails  
#     message["Subject"] = subject
#     if bcc is not None:
#       message["Bcc"] = ','.join(bcc) if isinstance(bcc, list) else bcc
#     # Add body to email
#     message.attach(MIMEText(body, "plain")) #if plain text
#     #message.attach(MIMEText(body, "html")) #if html
#     if attachments is not None:
#       for attachment in attachments:
#         filname = attachment.split('/')[-1]
#         with open(attachment, "rb") as file:
#           # Add file as application/octet-stream
#           # Email client can usually download this automatically as attachment
#           part = MIMEBase("application", "octet-stream")
#           part.set_payload(file.read())
#         # Encode file in ASCII characters to send by email
#         encoders.encode_base64(part)
#         # Add header as key/value pair to attachment part
#         part.add_header(
#           "Content-Disposition",
#           f"attachment; filename= {filename}",
#         )
#         # Add attachment to message and convert message to string
#         message.attach(part)    
#     text = message.as_string()
#     context = ssl.create_default_context()
#     with smtplib.SMTP("smtpdc.aa.com", "25") as server:
#       server.sendmail(sender_email, receiver_emails, text)

# # Register the UDF
# SendEmail = spark.udf.register("SendEmail", send_email)

# COMMAND ----------

# DBTITLE 1,Send Email
# def send_email(sender_email, receiver_emails, subject, body, bcc=None, attachments=None, string_values = None):
#     import email, smtplib, ssl
#     from email import encoders
#     from email.mime.base import MIMEBase
#     from email.mime.multipart import MIMEMultipart
#     from email.mime.text import MIMEText  
#     # Create a multipart message and set headers
#     message = MIMEMultipart()
#     message["From"] = sender_email  
#     message["To"] = ','.join(receiver_emails) if isinstance(receiver_emails, list) else receiver_emails  
#     message["Subject"] = subject
#     if bcc is not None:
#       message["Bcc"] = ','.join(bcc) if isinstance(bcc, list) else bcc
#     # Add body to email
#     message.attach(MIMEText(body, "plain")) #if plain text
#     #message.attach(MIMEText(body, "html")) #if html
#     filename = ''
#     if attachments is not None:
#       for attachment in attachments:
#         filname = attachment.split('/')[-1]
#         with open(attachment, "rb") as file:
#           # Add file as application/octet-stream
#           # Email client can usually download this automatically as attachment
#           part = MIMEBase("application", "octet-stream")
#           part.set_payload(file.read())
#         # Encode file in ASCII characters to send by email
#         encoders.encode_base64(part)
#         # Add header as key/value pair to attachment part
#         part.add_header(
#           "Content-Disposition",
#           f"attachment; filename= {filename}",
#         )
#         # Add attachment to message and convert message to string
#         message.attach(part)
    
#     if string_values is not None:
#       for string_value in string_values:
#         filename = string_value.split('|')[0]
#         part = MIMEBase("application", "octet-stream")
#         part.set_payload(string_value.split('|')[1].encode("utf-8"))
  
#         # Encode file in ASCII characters to send by email
#         encoders.encode_base64(part)
#         # Add header as key/value pair to attachment part
#         part.add_header(
#           "Content-Disposition",
#           f"attachment; filename= {filename}",
#         )
#         # Add attachment to message and convert message to string
#         message.attach(part)
#     text = message.as_string()
#     context = ssl.create_default_context()
#     with smtplib.SMTP("smtpdc.aa.com", "25") as server:
#       server.sendmail(sender_email, receiver_emails, text)

# # Register the UDF
# SendEmail = spark.udf.register("SendEmail", send_email)

# COMMAND ----------

# DBTITLE 1,Log Operational Metadata
from datetime import datetime
from decimal import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

def log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH):
  from datetime import datetime,timedelta,date
  
  
  DataSchema = StructType([StructField("SYS_NM", StringType()),
                           StructField("NOTEBOOK_NM", StringType()),
                           StructField("CLUSTER_NM", StringType()),
                           StructField("CLUSTER_ID", StringType()),
                           StructField("SRC_FILE_NM", StringType()),
                           StructField("TARGET_NM", StringType()),
                           StructField("START_TMS", StringType()),
                           StructField("END_TMS", StringType()),
                           StructField("TARGET_TYPE_CD", StringType()),
                           StructField("STATUS_CD", StringType()),
                           StructField("MSG_DESC", StringType()),
                           StructField("TARGET_ADLS_ZONE", StringType()),
                           StructField("INSERTED_CT", DecimalType()),
                           StructField("UPDATED_CT", DecimalType()),
                           StructField("DELETED_CT", DecimalType()),
                           StructField("LOG_DATE", StringType()),
                           StructField("RUN_ID", StringType()),
                           StructField("NOTEBOOK_JOB_URL", StringType()),
                           StructField("SAVE_PATH", StringType())])
  
  INSERTED_CT = 0
  UPDATED_CT = 0
  DELETED_CT = 0
  
  Operation = ""
  
  END_TMS = str(datetime.now())
  
  FSBCH_APPEND_LIST = []
  
  if(STATUS_CD == 'S'): #Success
    if(TARGET_NM != ""):
      db_name = TARGET_NM.split('.')[0]
      table_name = TARGET_NM.split('.')[1]
      #print(db_name)
      #print(table_name)
      
      if (sqlContext.sql("""show tables in {0}""".format(db_name.lower()))
          .filter(col("tableName") == table_name.lower())
          .count() > 0):
        Operation = sqlContext.sql("""select operation from (describe history {0}) t LIMIT 1""".format(TARGET_NM)).collect()[0][0]
      
      if(Operation == 'WRITE'):
        INSERTED_CT = sqlContext.sql("""select case when operation = 'WRITE' then operationMetrics.numOutputRows when operation = 'UPDATE' then operationMetrics.numUpdatedRows when operation = 'DELETE' then operationMetrics.numDeletedRows when operation = 'MERGE' then (operationMetrics.numTargetRowsInserted + operationMetrics.numTargetRowsUpdated + operationMetrics.numTargetRowsDeleted) end as INSERTED_CT from (describe history {0}) t LIMIT 1""".format(TARGET_NM)).collect()[0][0]
        
        if INSERTED_CT is None:
          INSERTED_CT = 0
          print("Inserted Record count: "+str(INSERTED_CT))
      
      elif(Operation == 'UPDATE'):
        UPDATED_CT = sqlContext.sql("""select case when operation = 'WRITE' then operationMetrics.numOutputRows when operation = 'UPDATE' then operationMetrics.numUpdatedRows when operation = 'DELETE' then operationMetrics.numDeletedRows when operation = 'MERGE' then (operationMetrics.numTargetRowsInserted + operationMetrics.numTargetRowsUpdated + operationMetrics.numTargetRowsDeleted) end as INSERTED_CT from (describe history {0}) t LIMIT 1""".format(TARGET_NM)).collect()[0][0]
        
        if UPDATED_CT is None:
          UPDATED_CT = 0
          print("UPDATED Record count: "+str(UPDATED_CT))
      
      elif(Operation == 'DELETE'):
        DEL_CT = sqlContext.sql("""select case when operation = 'WRITE' then operationMetrics.numOutputRows when operation = 'UPDATE' then operationMetrics.numUpdatedRows when operation = 'DELETE' then operationMetrics.numDeletedRows when operation = 'MERGE' then (operationMetrics.numTargetRowsInserted + operationMetrics.numTargetRowsUpdated + operationMetrics.numTargetRowsDeleted) end as INSERTED_CT from (describe history {0}) t LIMIT 1""".format(TARGET_NM)).collect()[0][0]
        
        if DEL_CT is None:
          DELETED_CT = 0
          print("DELETED Record count: "+str(DELETED_CT))
      
      elif(Operation == 'MERGE'):
        RECORD_CT = sqlContext.sql("""select operationMetrics.numTargetRowsInserted as INSERTED_CT, operationMetrics.numTargetRowsUpdated as UPDATED_CT, operationMetrics.numTargetRowsDeleted as DEL_CT from (describe history {0}) t LIMIT 1""".format(TARGET_NM)).rdd.collect()[0]
        
        RECORD_CT_L = str(RECORD_CT).split(',')
        INSERTED_CT = (RECORD_CT_L[0].split("'"))[1].split("'")[0]
        UPDATED_CT = (RECORD_CT_L[1].split("'"))[1].split("'")[0]
        DEL_CT = (RECORD_CT_L[2].split("'"))[1].split("'")[0]
        
        if INSERTED_CT is None:
          INSERTED_CT = 0
        if UPDATED_CT is None:
          UPDATED_CT = 0
        if DEL_CT is None:
          DELETED_CT = 0
      
      else:
        INSERTED_CT = 0
        UPDATED_CT = 0
        DELETED_CT = 0
      
  print("Inserted Record count: "+str(INSERTED_CT))
  print("UPDATED Record count: "+str(UPDATED_CT))
  print("DELETED Record count: "+str(DELETED_CT))
    
  TARGET_NM = TARGET_NM.replace("'", "")
  
  FSBCH_TEMP = [SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, Decimal(INSERTED_CT), Decimal(UPDATED_CT), Decimal(DELETED_CT), str(datetime.now())[0:10], RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH]
  #print(FSBCH_TEMP)
 
  FSBCH_APPEND_LIST.append(FSBCH_TEMP)
  #print(FSBCH_APPEND_LIST[0])
  
  FSBCH_DF = spark.createDataFrame(FSBCH_APPEND_LIST, schema=DataSchema)
  #FSBCH_DF = FSBCH_DF.withColumn('LOG_DT', lit(str(datetime.now()).replace("-", "").replace(" ", "_").replace(":", "").replace(".", "_")))
  FSBCH_DF = FSBCH_DF.withColumn('LOG_DT', lit(str(datetime.now())[0:10]))
  FSBCH_DF = FSBCH_DF.withColumn('SYS_NM', lit(SYS_NM))
  #display(FSBCH_DF)
  
  #FSBCH_DF.coalesce(1).write.mode("append").format("json").partitionBy("SYS_NM","LOG_DT").save(SAVE_PATH)
  FSBCH_DF.coalesce(1).write.mode("append").format("delta").partitionBy("SYS_NM","LOG_DT").save(SAVE_PATH)
  
  print("Successfully Logged operational metadata")

# COMMAND ----------

# log_operational_data('oasis', 'NOTEBOOK_NM', 'CLUSTER_NM', 'CLUSTER_ID', 'SRC_FILE_NM', 'oasis_struct.fi', 'START_TMS', 'END_TMS', 'TARGET_TYPE_CD', 'S', 'MSG_DESC', 'TARGET_ADLS_ZONE', 'RUN_ID', 'SAVE_PATH')

# COMMAND ----------

# DBTITLE 1,Get the Drain File name from Conversion Report
def get_drain_file_name(conv_report_file_path):
  oaasisDataFileName= ''
  
  try:
    oaasisDataFileName = list(spark.read.option("multiline", "true").json(conv_report_file_path + "conversionReport*.json").select("DataFileName").toPandas()['DataFileName'])[0]
  except Exception as e:
    # dont do anything
    print(oaasisDataFileName)
    
  return oaasisDataFileName

# COMMAND ----------

# get_drain_file_name("abfss://oasis@aabaoriondlsnp.dfs.core.windows.net/raw/")

# COMMAND ----------

# DBTITLE 1,Get the Notebook job execution URL
import json

def get_notebook_job_url():
  notebook_job_url = ''
  
  context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
  context = json.loads(context_str)
  
  adb_url = context.get('tags', {}).get('browserHostName', '')
  
  org_id = context.get('tags', {}).get('orgId', '')
  
  run_id_obj = context.get('currentRunId', {})
  run_id = run_id_obj.get('id', '') if run_id_obj else "Manual Run"
  
  job_id = context.get('tags', {}).get('jobId', '') or "Manual Run"
  
  if(job_id != 'Manual Run'):
    if(run_id != 'Manual Run'):
      if(adb_url != ''):
        notebook_job_url =  'https://'+str(adb_url)+'/?o='+str(org_id)+'#job/'+str(job_id)+'/run/'+str(run_id)
      else:
        notebook_job_url =  'https://adb-'+str(org_id)+'.11.azuredatabricks.net'+'/?o='+str(org_id)+'#job/'+str(job_id)+'/run/1'
    else:
      notebook_job_url = 'Manual Run'
  else:
    notebook_job_url = 'Manual Run'
  
  #print(notebook_job_url)
  return (notebook_job_url)

# COMMAND ----------

# DBTITLE 1,Get the Notebook RunID
def get_notebook_run_id():
  notebook_run_id = ''  
  
  context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
  context = json.loads(context_str)
  run_id_obj = context.get('currentRunId', {})
  run_id = run_id_obj.get('id', None) if run_id_obj else 'Manual Run'
  
  if(run_id != 'Manual Run'):
    notebook_run_id = run_id
  else:
    notebook_run_id = 'Manual Run'
  return(notebook_run_id)

# COMMAND ----------

# DBTITLE 1,Get the Last Snapshot Date
def get_last_snapshot_date(file_path):
  snapshot_date = list(spark.read.option("multiline", "true").json(file_path + "oasis_last_snapshot.json").select("last_snapshot_date").toPandas()['last_snapshot_date'])[0]
  
  return snapshot_date

# COMMAND ----------

# get_last_snapshot_date('abfss://oasis@aabaoriondlsnp.dfs.core.windows.net/config/')

# COMMAND ----------

#function for restartability

import re
from pyspark.sql import Row

def restart_logic(SRC_FILE_NM,Indicator_File_Path,SUFFIX):
  status = ''
  m_SOURCE_FILE_NAME = re.search(r'(.*?(?P<SOURCE_FILE_NM>.*)\.json)', SRC_FILE_NM)
  SOURCE_FILE_NAME = m_SOURCE_FILE_NAME['SOURCE_FILE_NM'] if m_SOURCE_FILE_NAME else ''
  SOURCE_FILE_NAME_SUFFIX = SOURCE_FILE_NAME + "_" + SUFFIX + ".json"
  touch_file_name = Indicator_File_Path + SOURCE_FILE_NAME_SUFFIX
  file_present = dbutils.fs.ls(Indicator_File_Path)
  if(SOURCE_FILE_NAME_SUFFIX in str(file_present)):
    print("touch_File_aready_exist")
    status = "Skipped"
  else: 
    print("touch_File_does_not_exist")
    status = "Saved"
    write()
    touch_df = spark.createDataFrame([{"FILE_NAME":SOURCE_FILE_NAME_SUFFIX}])
    touch_df.write.format("json").mode('append').save(touch_file_name) 
    files = dbutils.fs.ls(touch_file_name)
    json_file = [x.path for x in files if x.path.endswith(".json")][0]
    dbutils.fs.mv(json_file, touch_file_name.rstrip('/') + " ")
    dbutils.fs.rm(touch_file_name, recurse = True)
  return status

# COMMAND ----------

#function for flatteing 1 to Many columns

from pyspark.sql import types as T
import pyspark.sql.functions as F

def flatten_many(df):
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])
    
    qualify = list(complex_fields.keys())[0] + "_"
    parent = ''

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode(col_name))
    
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])
        

    return df

# COMMAND ----------

# DBTITLE 1,Load Data from Databricks to Teradata using fastLoad
from pyspark.sql.functions import *
from pyspark.sql.types import *

def td_fastload(query, driver, hostName, database, tableName, userName, password, batchsize, sessions):
  
  spark.sql(query).coalesce(1).write.format('jdbc')\
  .option('driver', driver) \
  .option('url', "jdbc:teradata://{0}/ERROR_TABLE_DATABASE={1},TYPE=FASTLOAD".format(hostName, database)) \
  .option('dbtable', tableName) \
  .option('user', userName) \
  .option('password', password) \
  .option('batchsize', batchsize)\
  .option('sessions', sessions)\
  .mode('append')\
  .save()

# COMMAND ----------

# DBTITLE 1,Execute DML commands on Teradata
import teradatasql as td

def td_dml(query, hostName, userName, password):
  
  tdsession= td.connect(host=hostName, user=userName , password=password)
  tdcur = tdsession.cursor()
  insert_mosaic_query = query
  tdcur.execute(insert_mosaic_query)
  tdcur.close()

# COMMAND ----------

# DBTITLE 1,Installing O365 python package
# pip install O365

# COMMAND ----------

# DBTITLE 1,Importing necessary packages
# from O365 import Account
# from azure.storage.blob import BlobClient, BlobServiceClient
# from io import BytesIO

# COMMAND ----------

# secret_scope = "n-fsbch-sp-secret-scope"
# blob_key = "ba-n-fsbch-blob-key"
# storage_account_name = "banzeausfsbchstg004"
# blob_name = "APASTAT1_D210803_4mb.txt"
# container_name = "p-test-data"
# clinet_id = "e045c6a2-2fc2-4f58-9c40-8c0a19a3d517"
# app_key = "ba-n-fsbch-001-sp-secret"
# app_tenant_id = "49793faf-eb3f-4d99-a0cf-aef7cce79dc1"
# resource_id = "OrionCrew.NonProd@aa.com"
# receiver_emails = "amareswarareddy.chintalapalli@aa.com,achintalapal@dxc.com".split(",")
# subject = "Test"
# body = """
#     <html>
#         <body>
#             <strong>There should be an image here:</strong>
#         </body>
#     </html>
#     """

# COMMAND ----------

# DBTITLE 1,send email without attachment using O365 python package
from O365 import Account

def send_email(receiver_emails, subject, body, clinet_id, app_tenant_id, resource_id, secret_scope, app_key):
  
  #passing application credentials and authenticating
  credentials = (clinet_id, dbutils.secrets.get(scope = secret_scope, key = app_key))
  account = Account(credentials, auth_flow_type = "credentials", tenant_id = app_tenant_id)
  
  if account.authenticate():
    print("Authenticated!")
  
  m = account.new_message(resource = resource_id)
  m.to.add(receiver_emails)
  m.subject = subject
  m.body = body
  m.send()

# COMMAND ----------

# DBTITLE 1,send email with attachment using O365 python package
from io import BytesIO

def send_email_attachment_string(receiver_emails, subject, body, clinet_id, app_tenant_id, resource_id, secret_scope, app_key, blob = None, blob_name = None):
  
  #passing application credentials and authenticating
  credentials = (clinet_id, dbutils.secrets.get(scope = secret_scope, key = app_key))
  account = Account(credentials, auth_flow_type = "credentials", tenant_id = app_tenant_id)
  
  if account.authenticate():
    print("Authenticated!")
  
  m = account.new_message(resource = resource_id)
  m.to.add(receiver_emails)
  m.subject = subject
  m.body = body
  m.attachments.add([(BytesIO(blob.encode('utf-8')), blob_name)])
  
  m.send()

# COMMAND ----------

print("End of Notebook Reached")
