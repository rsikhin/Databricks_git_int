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

#Send email without attachment
send_email(receiver_emails, subject, body, clinet_id, app_tenant_id, resource_id, secret_scope, app_key)

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

#Send email with attachment
send_email_attachment(receiver_emails, subject, body, clinet_id, app_tenant_id, resource_id, secret_scope, app_key, blob_name, storage_account_name, container_name, blob_key)
