# Databricks notebook source
# DBTITLE 1,Installing O365 python package
pip install O365

# COMMAND ----------

# DBTITLE 1,Installing azure-storage-blob python package
pip install azure-storage-blob

# COMMAND ----------

# DBTITLE 1,Importing necessary packages
from O365 import Account
from azure.storage.blob import BlobClient, BlobServiceClient
from io import BytesIO

# COMMAND ----------

secret_scope = "n-fsbch-sp-secret-scope"
blob_key = "ba-n-fsbch-blob-key"
storage_account_name = "banzeausfsbchstg004"
blob_name = "APASTAT1_D210803_4mb.txt"
container_name = "p-test-data"
clinet_id = "e045c6a2-2fc2-4f58-9c40-8c0a19a3d517"
app_key = "ba-n-fsbch-001-sp-secret"
app_tenant_id = "49793faf-eb3f-4d99-a0cf-aef7cce79dc1"
resource_id = "OrionCrew.NonProd@aa.com"
receiver_emails = "amareswarareddy.chintalapalli@aa.com,achintalapal@dxc.com".split(",")
subject = "Test"
body = """
    <html>
        <body>
            <strong>There should be an image here:</strong>
        </body>
    </html>
    """

# COMMAND ----------

# DBTITLE 1,send email with attachment using O365 python package
def send_email_attachment(receiver_emails, subject, body, clinet_id, app_tenant_id, resource_id, secret_scope, app_key, blob_name = None, storage_account_name = None, container_name = None, blob_key = None):
  
  #passing application credentials and authenticating
  credentials = (clinet_id, dbutils.secrets.get(scope = secret_scope, key = app_key))
  account = Account(credentials, auth_flow_type = "credentials", tenant_id = app_tenant_id)
  
  if account.authenticate():
    print("Authenticated!")
  
  m = account.new_message(resource = resource_id)
  m.to.add(receiver_emails)
  m.subject = subject
  m.body = body
  
  #if blob_name(filename) is present then only add attachment
  if(blob_name is not None):
    #Extract key from Keyvault
    keys = dbutils.secrets.get(scope = secret_scope, key = blob_key)
    
    #set spark configuration with storage account name and key
    spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",keys)
    
    #Reading file contents using blob_service_client and blob_client
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName="+storage_account_name+";AccountKey="+keys+";EndpointSuffix=core.windows.net")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob= blob_name)
    blob = blob_client.download_blob().content_as_bytes()
    
    #Converting string to bytes and adding as an attachment
    m.attachments.add([(BytesIO(blob), blob_name)])
  
  m.send()

# COMMAND ----------

# DBTITLE 1,send email without attachment using O365 python package
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

# #receiver_emails_n = "amareswarareddy.chintalapalli@aa.com,pradeep.chamarthi@aa.com"
# receiver_emails_n = "amareswarareddy.chintalapalli@aa.com"
# receiver_emails = receiver_emails_n.split(",")
# print(receiver_emails)

# COMMAND ----------

#Send email without attachment
send_email(receiver_emails, subject, body, clinet_id, app_tenant_id, resource_id, secret_scope, app_key)

# COMMAND ----------

#Send email with attachment
send_email_attachment(receiver_emails, subject, body, clinet_id, app_tenant_id, resource_id, secret_scope, app_key, blob_name, storage_account_name, container_name, blob_key)

# COMMAND ----------

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
  
  #if blob_name(filename) is present then only add attachment
  if(blob_name is not None):
    m.attachments.add([(BytesIO(blob.encode('utf-8')), blob_name)])
  
  m.send()

# COMMAND ----------

#Send email with attachment
send_email_attachment_string(receiver_emails, subject, body, clinet_id, app_tenant_id, resource_id, secret_scope, app_key, "blob", blob_name)

# COMMAND ----------

# secret_scope = "n-fsbch-sp-secret-scope"
# blob_key = "ba-n-fsbch-blob-key"

# keys = dbutils.secrets.get(scope = secret_scope, key = blob_key)
# print(keys)

# storage_account_name = "banzeausfsbchstg004"
# spark.conf.set(
#   "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
#   keys)

# blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName="+storage_account_name+";AccountKey="+keys+";EndpointSuffix=core.windows.net")

# blob_name = "APASTAT1_D210803_4mb.txt"

# blob_client = blob_service_client.get_blob_client(container="p-test-data", blob= blob_name)
# print(blob_client)
# blob = blob_client.download_blob().content_as_bytes()
# #print(blob)

# COMMAND ----------

# #Admin credentials Needed
# from io import BytesIO

# credentials = ('e045c6a2-2fc2-4f58-9c40-8c0a19a3d517', dbutils.secrets.get(scope = "n-fsbch-sp-secret-scope", key = "ba-n-fsbch-001-sp-secret"))
# #credentials = ('e045c6a2-2fc2-4f58-9c40-8c0a19a3d517', "mDOvx5KMp~iDnS_DajM.9itT2-5cPa3-12")

# account = Account(credentials, auth_flow_type='credentials', tenant_id='49793faf-eb3f-4d99-a0cf-aef7cce79dc1')
# #scopes = ['https://graph.microsoft.com/Mail.Send']
# #account = Account(credentials, tenant_id='49793faf-eb3f-4d99-a0cf-aef7cce79dc1', scopes=scopes)

# if account.authenticate():
#     print('Authenticated!')
# m = account.new_message(resource='OrionCrew.NonProd@aa.com')
# m.to.add('amareswarareddy.chintalapalli@aa.com')
# #m.to.add(['amareswarareddy.chintalapalli@aa.com','pradeep.chamarthi@aa.com'])
# m.subject = 'Test'
# #m.body = 'message'
# body = """
#     <html>
#         <body>
#             <strong>There should be an image here:</strong>
#         </body>
#     </html>
#     """
# m.body = body
# #m.attachments.add("/dbfs/sample.txt")
# #m.attachments.add("/dbfs/FileStore/tables/APASTAT1_D210803_4mb.txt")

# image = BytesIO(blob)
# m.attachments.add([(image, blob_name)])

# #m.attachments.add(blob_client.download_blob().content_as_bytes())
# #part = MIMEBase("application", "octet-stream")
# #part.set_payload("string_value".encode("utf-8"))
# #encoders.encode_base64(part)
# #m.attachments.add([(part, 'my_file_image.png')])

# m.send()
