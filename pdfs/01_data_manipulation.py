#!/usr/bin/env python
# coding: utf-8

# # Simple Data Manipulation

# This notebook may not tell a story like the others, but it stands as a powerful demonstration of what's possible.
# 
# It's more like a catalog of data operation we can use to solve our toughest data problems

# ### Imports

# In[161]:


import pandas as pd
import json
from fuzzywuzzy import process


# ## Loading Data

# The container for all our data is the DataFrame, a Python object designed to hold tabular data. You can think of it like a table, an Excel sheet, or a matrix that makes it easy to manipulate data within Python
# 
# It can handle more than two dimensions when working with deep learning models or multi-dimensional datasets, such as cubed financial data or time-series with multiple variables.
# 
# Let's load some data!

# In[162]:


df_accounts = pd.read_csv('../datasets/accounts.csv') #you can specify delimeter, encoding and other parameters in the read_csv function.
df_contacts = pd.read_csv('../datasets/contacts.csv')
df_accounts_excel = pd.read_excel('../datasets/accounts_excel.xlsx', sheet_name='Sheet1') 


# ## Inspect

# We're going to explore different ways of slicing and dicing data in pandas. This is just the tip of the iceberg. In a programmatic environment, how you manipulate the data is limited only by your imagination.

# Let's count some rows

# In[163]:


row_counts = pd.DataFrame({
    'Accounts': [df_accounts.shape[0]],
    'Contacts': [df_contacts.shape[0]]
})
display(row_counts)
display(df_accounts.count())


# We can desribe the data using this method along with getting some sample data.

# In[164]:


display(df_contacts.describe()) # Descriptive statistics for numerical columns
display(df_contacts.sample(2)) #get some sample data, specify the number of rows you want to sample
display(df_accounts.head(3)) #get the first 3 rows of the dataframe, you can alos get the last by using tail()


# Filter rows by value, we are creating a new dataframe here, the original one is not modified

# In[165]:


vendors = df_accounts[df_accounts['Type'] == 'Vendor']
display(vendors[0:5])


# Select only certain columns to be used in a dataframe

# In[166]:


vendors = df_accounts[df_accounts['Type'] == 'Vendor'][['AccountName', 'Industry']]
display(vendors[0:5])


# Before going further lets join these datasets together, this will be the easiest merge you will likely ever deal with haha

# In[167]:


merged = pd.merge(df_contacts, df_accounts, on="AccountName", how="left")
display(merged.columns)


# Let's get creative and query the in ways that SOQL or Excel may have trouble doing.

# In[168]:


#Filter based on regex

df_contacts[df_contacts["Title"].str.contains(r"\b(VP|Vice President|Director|Head)\b", case=False, na=False)][0:5]


# In[169]:


# Hot Leads in Tech/Finance in CA/NY, in excel it needs multiple IF or FILTER functions, hard to maintain.
merged.pivot_table(index="Industry", columns="Rating", values="FirstName", aggfunc="count", fill_value=0)
merged[
    (merged["Rating"] == "Hot") &
    (merged["Industry"].isin(["Technology", "Finance"])) &
    (merged["MailingState"].isin(["CA", "NY"]))
]


# In[170]:


#Group by AccountName and aggregate with custom function
merged.groupby("AccountName").agg({
    "FirstName": "count",
    "Rating": lambda x: x.mode().iloc[0] if not x.mode().empty else "Unknown"
})[0:5]


# In[172]:


#This line calculates the percentage of each account rating within every region, reshapes it into a table, and fills in any missing values with zeros.
def assign_region(state):
    if state in ["CA", "WA", "OR"]:
        return "West"
    elif state in ["NY", "MA", "NJ"]:
        return "Northeast"
    elif state in ["TX", "FL"]:
        return "South"
    else:
        return "Other"

df_accounts["Region"] = df_accounts["BillingState"].apply(assign_region)
region_leads = df_accounts.groupby("Region")["Rating"].value_counts(normalize=True).unstack().fillna(0)
region_leads


# ## Data Manipulation

# Slicing and dicing data is great, but we also need to be able to add or rename columns, and perform other operation on the dataframe

# ### DataFrame Mods

# In[ ]:


# Dropping duplicate emails in contacts dataset
print('Original contact count:', df_contacts.shape[0])
df_contacts_nodup = df_contacts.drop_duplicates(subset='Email')
print('Contact count after dropping duplicate emails:', df_contacts_nodup.shape[0])
duplicated_emails = df_contacts[df_contacts.duplicated(subset='Email', keep=False)]['Email'].unique()
print('Duplicated emails:')
print(duplicated_emails)


# In[ ]:


#a simple way to assign a region based on state, however you could do this with a formula field on salesforce, but this is for demo purposes
def assign_region(state):
    if state in ['CA', 'WA', 'OR']:
        return 'West'
    elif state in ['NY', 'MA', 'NJ']:
        return 'Northeast'
    elif state in ['TX', 'FL']:
        return 'South'
    else:
        return 'Other'

df_accounts['Region'] = df_accounts['BillingState'].apply(assign_region)
df_accounts['Region'][0:3]


# In[ ]:


#replace values
df_contacts["LeadSource"] = df_contacts["LeadSource"].replace("Trade Show", "Event")
df_contacts["LeadSource"][0:5]


# In[ ]:


#this is used to rename the columns in a dataframe, inplace means it will change the original dataframe, I do not recommend using inplace 
# as it can lead to confusion, but it is useful for quick changes in a notebook
df_contacts.rename(columns={
    "Phone": "ContactPhone"
}, inplace=True)
df_contacts.columns


# In[ ]:


# Example: Add a new contact row to df_contacts
new_contact = {
    'FirstName': 'Emma',
    'LastName': 'Smith',
    'Email': 'emma.smith@example.com',
    'ContactPhone': '555-123-4567',
    'MobilePhone': '555-987-6543',
    'AccountName': 'Acme Corp',
    'Title': 'Account Manager',
    'MailingCity': 'San Diego',
    'MailingState': 'CA',
    'LeadSource': 'Web'
}
df_contacts = pd.concat([df_contacts, pd.DataFrame([new_contact])], ignore_index=True)
display(df_contacts.tail())


# In[ ]:


#Removing a row from a dataframe is simply at matter of exclusion
df_contacts = df_contacts[df_contacts['Email'] != 'emma.smith@example.com']
display(df_contacts.tail())


# ### Export Date to CSV

# In[173]:


df_contacts.to_csv('../datasets/contacts_export.csv', index=False)


# ### Bonus

# Flatten JSON easily into dataframe
{
  "organization": {
    "name": "SampleOrg",
    "accounts": [
      {
        "account": {
          "details": {
            "AccountName": "Blue Ocean Tech",
            "Industry": "Healthcare",
            "Type": "Vendor",
            "Rating": "Hot"
          },
          "location": {
            "BillingCity": "San Francisco",
            "BillingState": "WA"
          },
          "contact": {
            "Phone": "555-410-9237",
            "Website": "www.blueoceantech.com"
          }
        }
      }
    ]
  }
}

# In[ ]:


with open('../datasets/accounts.json') as f:
    data = json.load(f)

accounts = data['organization']['accounts']

flat_accounts = []
for entry in accounts:
    account = entry['account']
    flat = {}
    flat.update(account.get('details', {}))
    flat.update({f"Billing{key[7:]}": val for key, val in account.get('location', {}).items()})
    flat.update(account.get('contact', {}))
    flat_accounts.append(flat)

df_flat = pd.DataFrame(flat_accounts)
display(df_flat.head())


# In[ ]:


# output the newly created flat dataframe to a new json structure, all for funzies
#You can also work with XML but that is a different story
subset = df_flat.head(2)
output = []

for _, row in subset.iterrows():
    entry = {
        'AccountName': row['AccountName'],
        'Industry': row['Industry'],
        'Type': row['Type'],
        'Rating': row['Rating'],
        'BillingCity': row['BillingCity'],
        'BillingState': row['BillingState'],
        'contact': {
            'Phone': row['Phone'],
            'Website': row['Website']
        }
    }
    output.append(entry)

print(json.dumps(output, indent=2))


# In[ ]:


#simple fuzzy matching example using fuzzywuzzy

account_names = df_accounts['AccountName'].tolist()
matches = []

for name in account_names:
    results = process.extract(name, account_names, limit=5)
    for match_name, score in results:
        if name != match_name and score >= 90:
            matches.append({'AccountName': name, 'Match': match_name, 'Score': score})

fuzzy_matches_df = pd.DataFrame(matches).drop_duplicates()
display(fuzzy_matches_df)


# ## Export Notebook

# In[177]:


import os
notebook_path = os.path.basename(get_ipython().getoutput('echo $PWD')[0]) + '/' + os.path.basename(get_ipython().getoutput('echo $PWD')[0]) + '.ipynb'
notebook_file = os.path.basename(get_ipython().getoutput('ls *.ipynb')[0])
os.makedirs('pdfs', exist_ok=True)
os.makedirs('html', exist_ok=True)
get_ipython().system(f'jupyter nbconvert --to html "{notebook_file}" --output "html/{os.path.splitext(notebook_file)[0]}"')
get_ipython().system(f'jupyter nbconvert --to pdf "{notebook_file}" --output "pdfs/{os.path.splitext(notebook_file)[0]}"')


# In[181]:


get_ipython().system("get_ipython().system('jupyter nbconvert --to html 01_data_manipulation.ipynb')")

