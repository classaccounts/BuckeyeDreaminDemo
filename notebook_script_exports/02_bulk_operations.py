#!/usr/bin/env python
# coding: utf-8

# # Bulk Operations
# 
# In this example, we will look at bulk operations using simple saleforce

# ## Imports

# In[7]:


import pandas as pd
from simple_salesforce import Salesforce
import os
import io
from dotenv import load_dotenv
import ipywidgets as widgets
from IPython.display import display

load_dotenv()


# ## Authenticate

# Always store your credentials in environment variables, and always use a service account.
# 
# The `simple-salesforce` library supports multiple authentication methods:
# 
# - Username, password, and security token  
# - Session ID and instance URL  
# - OAuth 2.0 (JWT, web flow, or refresh token)  
# - Connected App credentials (via external libraries) 

# In[8]:


sf_domain = os.getenv('sf_domain')
sf_username = os.getenv('sf_username')
sf_password = os.getenv('sf_password')
sf_token = os.getenv('sf_token')


sf = Salesforce(
    username=sf_username,
    password=sf_password,
    security_token=sf_token,
    domain=sf_domain

)


# ## Prepare Data

# ### Load Data

# In[9]:


df = pd.read_csv('../datasets/parts.csv')
df.sample(5)


# ###  Clean Data

# In[10]:


#drop duplicate part numbers
df = df.drop_duplicates(subset='Part Number')


# In[11]:


#rename columns to match Salesforce field names
df = df.rename(columns= {
    'Part Number': 'Name',
    'Manufacturer': 'Manufacturer_Name__c',
    'Description': 'inscor__Keyword__c',
    'External Id': 'BD_ExternalId__c'
})
df.sample(3)


# ## Bulk API Operations

# ### Common Functions
# 

# The following code defines a helper function to process the results of Salesforce Bulk API v2 operations. It iterates through each job result, retrieves both successful and failed records, and converts them from CSV strings to pandas DataFrames. The function then adds the job ID to each record and concatenates all results into combined DataFrames for successes and failures. Finally, it returns these DataFrames for further analysis or reporting.

# In[12]:


def get_bulk2_results(result):
    combined_failed = pd.DataFrame()
    combined_success = pd.DataFrame()

    for job in result:
        job_id = job['job_id']
        failed = sf.bulk2.Product2.get_failed_records(job_id)
        success = sf.bulk2.Product2.get_successful_records(job_id)

        #since the results are returned as CSV strings, we need to convert them to DataFrames
        success = pd.read_csv(io.StringIO(success))
        failed = pd.read_csv(io.StringIO(failed))

        failed['job_id'] = job_id
        success['job_id'] = job_id

        combined_failed = pd.concat([combined_failed, failed], ignore_index=True)
        combined_success = pd.concat([combined_success, success], ignore_index=True)


        return combined_success, combined_failed


# ### Insert

# Converts a dataframe to a list of dictionaries (csv in a json format) and inserts the records into Salesforce using the Bulk API 2.0 with up to 100 concurrent threads. The results are split into successful and failed inserts, then printed and previewed.

# In[13]:


records=df.to_dict(orient='records')
result = sf.bulk2.Product2.insert(records=records, concurrency=100)
print(result)

#check the results
success, fail = get_bulk2_results(result)
print(f"Total records inserted: {len(success)}")
display(success.head(3))
print(f"Total records failed: {len(fail)}")
display(fail.head(3))


# ### Upsert

# Updates the dataset by replacing all 'TBA' manufacturer names with 'Boeing', then appends two new product rows with JAMCO as the manufacturer.

# In[14]:


#update the dateset so TBA manufacturers are set to Boeing
df['Manufacturer_Name__c'] = df['Manufacturer_Name__c'].replace('TBA', 'Boeing')
#or use the datframe replace method loc
#df.loc[df['Manufacturer_Name__c'] == 'TBA', 'Manufacturer_Name__c'] = 'Boeing'
#add a new row to the dataset in a similar format to the existing rows
new_rows = pd.DataFrame([
    {'Name': 'ABC123', 'Manufacturer_Name__c': 'JAMCO', 'inscor__Keyword__c': 'WIDGET', 'BD_ExternalId__c': 99999},
    {'Name': 'XYZ789', 'Manufacturer_Name__c': 'JAMCO', 'inscor__Keyword__c': 'GADGET', 'BD_ExternalId__c': 88888}
])

df = pd.concat([df, new_rows], ignore_index=True)


# Converts the dataframe to a list of dictionaries and upserts the records into Salesforce using BD_ExternalId__c as the external ID. Displays the results, separates successes from failures, and saves the successful records to a CSV file.

# In[15]:


#convert the DataFrame to a list of dictionaries
records = df.to_dict(orient='records')
result = sf.bulk2.Product2.upsert(records=records, external_id_field='BD_ExternalId__c')
print(result)

success, fail = get_bulk2_results(result)
print(f"Total records inserted: {len(success)}")
display(success.head(3))
print(f"Total records failed: {len(fail)}")
display(fail.head(3))

#only for update demonstration next, save sucess results
success.to_csv('../datasets/success_parts.csv', index=False)


# # Update

# In[16]:


#load and inspect the data
df = pd.read_csv('../datasets/success_parts.csv')
display(df.head(1))
print(f"Total records in frame: {len(df)}")


# Filters the dataset to only include records where the manufacturer is 'None Specified', keeping just the ID and manufacturer columns. Renames the ID column and updates the manufacturer to 'Salesforce Aviation' for all remaining rows.

# In[17]:


#Let's clean up dataset and only get the data we need
df = df[df['Manufacturer_Name__c'] == 'None Specified'][['sf__Id', 'Manufacturer_Name__c']]
df = df.rename(
   columns = {'sf__Id': 'Id'}
)
display(df.head(1))
print(f"Total records in frame: {len(df)}")

#update all manufacturer non speicified to Salesforce Aviation
df['Manufacturer_Name__c'] = 'Salesforce Aviation'
display(df.head(1))


# Converts the cleaned dataframe to a list of dictionaries and updates the corresponding Product2 records in Salesforce using their IDs. The results are processed to show successful and failed updates, with samples displayed for each.

# In[18]:


#convert the DataFrame to a list of dictionaries
#update the records based on the salesforce Id
records = df.to_dict(orient='records')
result = sf.bulk2.Product2.update(records=records)
print(result)

success, fail = get_bulk2_results(result)
print(f"Total records inserted: {len(success)}")
display(success.head(3))
print(f"Total records failed: {len(fail)}")
display(fail.head(3))


# ## Get Data
# 

# ### Download

# Defines a SOQL query to retrieve products with the manufacturer set to 'Salesforce Aviation' and downloads the results into the ../datasets folder using the Bulk API 2.0.

# In[19]:


soql_query = """
SELECT Name, Manufacturer_Name__c, inscor__Keyword__c, BD_ExternalId__c
FROM Product2
WHERE Manufacturer_Name__c = 'Salesforce Aviation'
"""
#there is another option to download this into a memory object, but this is easier.
sf.bulk2.Account.download(
    soql_query, path='../datasets', max_records=200000,
)


# ## Delete & Hard Delete

# Reads in the list of successfully inserted product records, extracts their IDs, and writes them to a CSV required for the hard delete operation. A confirmation button is displayed to trigger the delet

# In[ ]:


success = pd.read_csv('../datasets/success_parts.csv')
success = success.rename(columns={'sf__Id': 'Id'})
csv_path = '../datasets/delete_ids.csv'
success[['Id']].to_csv(csv_path, index=False)

button = widgets.Button(description='Confirm Hard Delete', button_style='danger')

def on_button_click(b):
    print('Running hard delete...')
    #result = sf.bulk2.Product2.hard_delete(csv_file=csv_path) for normal delete you can simply change the method to delete
    result = sf.bulk2.Product2.hard_delete(csv_file=csv_path)
    print('Done:', result)

button.on_click(on_button_click)
display(button)

#Write to CSV in ../dataset — required because simple-salesforce hard_delete asserts csv_file is not None
#Even though records= is accepted, it's not respected internally though a downstream assertion that csv_file is not None for delete operations.
#I'm working on a PR to fix this bug in the simple-salesforce repo
#If you are doing hard delete you will to enable that permission in your profile, do not be silly with that permission


# show dynamic get attr
# show wait function
# add notebook export

# ## Bonus
# 
# Using getattr dynamically selects the Salesforce object by name, and setting wait=False submits the job without blocking so results can be retrieved later using the job ID.

# In[21]:


# Dynamically choose the Salesforce object
object_name = 'Product2'
sf_object = getattr(sf.bulk2, object_name)

# Submit update request without waiting for results
records = df.to_dict(orient='records')
#job_info = sf_object.update(records=records, wait=False)  # non-blocking

# You can poll later using get_bulk2_results
print('Job submitted. Polling for results later...')
job_id = job_info['id']

# Later in the script...
success, fail = get_bulk2_results({'id': job_id})
print(f"Total records updated: {len(success)}")
display(success.head(3))
print(f"Total records failed: {len(fail)}")
display(fail.head(3))


# In[ ]:


get_ipython().system('jupyter nbconvert --to html "02_bulk_operations.ipynb" --output-dir=../html')
get_ipython().system('jupyter nbconvert --to script "02_bulk_operations.ipynb" --output-dir=../notebook_script_exports')

