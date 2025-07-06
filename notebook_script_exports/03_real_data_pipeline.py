#!/usr/bin/env python
# coding: utf-8

# # Bid Winner

# All data contained in this repository, as well as any information queried or conveyed through its use, is publicly accessible and readily available on the internet. No proprietary, confidential, or restricted data is included or utilized.
# 
# This concept was created in my free time and is not used at my employer (toy project).

# ## The Process

# ### Problem Statement

# As a commercial aircraft parts distributor, your team relies on Salesforce to issue thousands of quotes daily, tapping into live inventory, consignment stock, and distributor agreements. But now that your company is entering government contracting, you are faced with a flood of bids from platforms like SAM.gov, each requiring manual cross-referencing with sources like PubLog FLIS, PartsBase, and parsed bid documents. Your quoting process depends on starting in Salesforce, where all critical data lives, but instead, teams are scraping external platforms, tracking bids in spreadsheets, and asking to embed Excel into Salesforce just to keep up. This not only breaks your data model, it threatens to shift your quoting process outside of Salesforce entirely. Without a way to bring this external data into Salesforce natively and efficiently, your team risks building shadow systems that cannot scale, are not secure, and undermine the very CRM you are investing in.
# 
# Do not let hard-to-reach data sources become the reason your process breaks down or slips outside the system designed to manage it.

# ### Data Flow
# 
# #### Find Bids
# 
# <img src="../images/sam.png" alt="SAM.gov Screenshot" width="400"/>
# 
# Bids from the Defense Logistics Agency are published on DIBBS and SAM.gov, requiring employees to manually filter through them to find aircraft parts they can procure. The process involves time-consuming sorting, digging through unstructured PDF documents, and interpreting vague or inconsistent descriptions of the required parts, all done outside of Salesforce.
# 
# #### Cross Reference on Publog Flis
# 
# <img src="../images/publog.png" alt="SAM.gov Screenshot" width="400"/>
# 
# Some bids only provide the NIIN, while distributors typically identify parts using the part number and manufacturer. This forces the team to manually cross-reference the public Federal Logistics Information System (PubLogFLIS) to find the corresponding part number, CAGE code, and other critical sourcing details. This application is older than I am.
# 
# #### (Optional) Cross Reference on Marketplace
# 
# If the part is not currently in inventory but has been sourced previously, your team can reference industry marketplaces to verify availability and gather supporting data.
# 
# #### Move to Source of Record (Saleforce)
# 
# Once all of this data is compiled, the team currently manages it in manual spreadsheets due to the volume and complexity of information. But to maintain consistency, visibility, and scalability, it needs to live in Salesforce as the system of record.

# ### Applicable Techniques to Any Pipeline
# 1. Using chatgpt to parse untructured data and get structued data in return.
# 2. Using Python to access any hard to reach data source and web APIs
# 3. Using the simple salesforce library to upload data into salesforce.

# ## Imports
# and some configuration

# In[187]:


import sys
sys.path.insert(0, '../scripts')
from govspend_search import GoveSpendSearch
from rfp_parser import RFPParse
from flis_search import FlisSearch
from parts_base import PartsBase
from simple_salesforce import Salesforce
import pandas as pd
import os
from dotenv import load_dotenv
import io
from datetime import datetime, timezone
load_dotenv()

search_id = "679aba32c9c299c7b31ade4f"
PATH_TO_DLL = "C:\\Users\\jackmchugh\\Downloads\\PublogDVD\\TOOLS\\MS12\\DecompDl64.dll"
PATH_TO_FEDLOG = "C:\\Users\\jackmchugh\\Downloads\\PublogDVD"

output_dir = "../datasets/demo_output/"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "target_list.csv")
output_manufacturer_file = os.path.join(output_dir, "manufacturer_information.csv")


# ## Helper Classes/Methods

# - **pb = PartsBase()**  
#     Instantiates the PartsBase helper, which is used to query live inventory and availability from the PartsBase marketplace API.  
#     This allows you to check if a part is available in the broader industry, supplementing your Salesforce product data.
# 
# - **govspend = GoveSpendSearch()**  
#     Instantiates the GoveSpendSearch helper, which is used to search and retrieve government bid opportunities from platforms like SAM.gov.  
#     This enables automated ingestion of bid data that would otherwise require manual entry or scraping.
# 
# - **flis = FlisSearch(path_to_dll=PATH_TO_DLL, path_to_fedlog=PATH_TO_FEDLOG)**  
#     Instantiates the FlisSearch helper, which wraps access to the Federal Logistics Information System (FLIS) data.  
#     It uses a DLL to parse the official government FLIS DVD, allowing you to cross-reference NIINs/NSNs to part numbers and manufacturers, which is critical for mapping government data to your Salesforce product catalog.
# 
# - **rfp_parser = RFPParse()**  
#     Instantiates the RFPParse helper, which uses AI/NLP to extract structured data (like part numbers, quantities, and requirements) from unstructured bid descriptions or PDF documents.  
#     This automates the process of turning raw government bid text into Salesforce-ready data fields.
# 

# In[188]:


pb = PartsBase()
govspend = GoveSpendSearch()
flis = FlisSearch(path_to_dll=PATH_TO_DLL, path_to_fedlog=PATH_TO_FEDLOG)
rfp_parser = RFPParse()


# Authenticate to Salesforce

# In[189]:


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


# Common method to get results from the Bulk2 API, we used this in the previous notebook.

# In[190]:


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


# ## Process Data

# ### Run GovSpend Search

# We need to gather bids to process from GovSpend and SAM.gov. We have a saved search in GovSpend that filters for the bids we care about, specifically those issued by DLA within the past day and matching our NAICS codes.

# In[191]:


govspend_result = govspend.search(search_id)
combined_result_list = []
manufacturer_result_list = []


# ### Process Bids

# This block loops through each bid from GovSpend and uses OpenAI to parse unstructured bid descriptions into structured fields like part number, quantity, and approved sources. It also enriches the data with part details from FLIS. If no part number is found directly, it tries to fall back to alternatives or uses a placeholder. The result is a clean dictionary of data for each bid, ready to be uploaded into Salesforce. Manufacturer details are also collected when available.

# In[192]:


count = 0

for bid in govspend_result['result']:
    parse_result = rfp_parser.parse_rfp(bid['description'])
    flis_result = flis.get_part_number_and_description(parse_result["niin"])

    ref_pn = (
        parse_result.get("part_number", "").strip()
        or flis_result.get("part_number", "").split(",")[0].strip()
        or "NO PART NUMBER"
    )

    #num_pb_results = pb.get_live_qty(ref_pn) I no longer have access to the PartsBase API, so this line is commented out.
    num_pb_results = 0  # Placeholder for PartsBase results

    result = {
        "bid_number": bid["bidNumber"],
        "ref_pn": ref_pn,
        "parse_desc": parse_result["description"],
        "qty_requested": parse_result["quantity"],
        "num_qty_on_pb": num_pb_results,
        "approved_source": parse_result["approved_source"],
        "lead_time": parse_result["lead_time"],
        "incumbent": parse_result["incumbent"],
        "multiple_award": parse_result["multiple_award"],
        "approved_source_codes": parse_result["approved_source_codes"],
        "date_create": bid["postedDate"],
        "date_due": bid["dueDate"],
        "psc_code": bid["PSCCode"],
        "set_aside": bid["setAside"],
        "NSN": parse_result["nsn"],
        "flis_pn": flis_result["part_number"],
        "parse_pn": parse_result["part_number"],
        "flis_desc": parse_result["description"]
    }

    print(result)
    count += 1
    print(f"Number of BIDs processed", count)
    combined_result_list.append(result)

    manufacturer_data = flis.get_cage_codes_and_pricing(parse_result["niin"])
    if not manufacturer_data.empty:
        manufacturer_result_list.append(manufacturer_data)


# ### Save the Results

# Saves the parsed bid data and manufacturer information into separate CSV files for later processing or import.

# In[193]:


df = pd.DataFrame(combined_result_list)
df.to_csv(output_file, index=False)
print(f"Data saved to {output_file}")

if manufacturer_result_list:
    df_manufacturer = pd.concat(manufacturer_result_list, ignore_index=True)
    df_manufacturer.to_csv(output_manufacturer_file, index=False)
    print(f"Manufacturer data saved to {output_manufacturer_file}")


# ## Bring Into Salesforce
# 
# We will be adding these bids to salesforce as a customer quote for Acme Inc.

# In[194]:


#Look at what we are dealing with in the dataframe and count the number of bids processed
print(f"Total number of bids processed: {len(df)}")
df.head(1)


# ### Wrangle the Data

# Creates a concatenated comment field, renames columns to match Salesforce field names, extracts a subset for parts with truncated keyword length, and displays a preview of the prepared quote lines.

# In[195]:


df['inscor__Internal_Comments_rt__c'] = df.astype(str).agg(' | '.join, axis=1)

df_quote_lines = df.rename(columns={
    "bid_number": "inscor__Customer_Reference_Line__c",
    "ref_pn": "Name",
    "parse_desc": "inscor__Keyword__c",
    "qty_requested": "inscor__Quantity_Requested__c",
    "num_qty_on_pb": "Number of Quantity on PartsBase",
    "approved_source": "Approved Source",
    "lead_time": "inscor__Lead_Time__c",
    "incumbent": "Incumbent",
    "multiple_award": "Multiple Award",
    "approved_source_codes": "Approved Source Codes",
    "date_create": "Date Created",
    "date_due": "inscor__Quote_Due__c",
    "psc_code": "PSC Code",
    "set_aside": "Set Aside",
    "NSN": "inscor__NSN1__c",
    "flis_pn": "FLIS Part Number",
    "parse_pn": "Parsed Part Number",
    "flis_desc": "FLIS Description"
})

df_parts = df_quote_lines[['Name', 'inscor__Keyword__c', 'inscor__NSN1__c']].copy() #hard copy
df_parts['inscor__Keyword__c'] = df_parts['inscor__Keyword__c'].astype(str).str[:14] #max length salesforce will allow is 15

df_quote_lines.head()


# Runs a SOQL query to download up to 2 million Product2 records using the Bulk API, saves the results to a CSV file, and loads them into a pandas DataFrame for local processing.
# 
# It’s not recommended to pull down that many rows on every run, especially in production environment. Please form a caching stratagy if you have to run this frequently (delta pulling or CDC)
# 

# In[196]:


sql_query = """
SELECT Id, Name
FROM Product2
"""

result = sf.bulk2.Account.download(
    sql_query,
    path='../datasets',
    max_records=2000000,
)

file_path = result[0]['file']
sf_parts = pd.read_csv(file_path)


# Filters df_parts to include only records with part names not already present in sf_parts, preparing a list of new parts to insert.

# In[197]:


df_insert = df_parts[~df_parts['Name'].isin(sf_parts['Name'])]
df_insert.head()


# Checks if there are any new parts to insert by evaluating if df_insert is not empty. If there are, it performs a bulk insert, logs the results, and updates the local cache of known parts.

# In[198]:


#add an if statment that if df insert has no rows, then skip the bulk insert
if not df_insert.empty:
    print("No new parts to insert. Skipping bulk insert.")

    records=df_insert.to_dict(orient='records')
    result = sf.bulk2.Product2.insert(records=records, concurrency=100)
    print(result)

    #check the results
    success, fail = get_bulk2_results(result)
    print(f"Total records inserted: {len(success)}")
    display(success.head(3))
    print(f"Total records failed: {len(fail)}")
    display(fail.head(3))

    #Yes you could (should) certainly use the rest API for this amount of data, but I already put BulkAPI in the presentation title
    #There is no backing out now.

    #add success results to the the existing parts dataframe
    #rename sf__Id to sf__Id for consistency
    success = success.rename(columns={'sf__Id': 'Id'})
    sf_parts = pd.concat([sf_parts, success[['Id', 'Name']]], ignore_index=True)
    #Drop any duplicate part names, part numbers should be unique.


# Removes duplicate part names from the Salesforce parts dataset, then merges it with the quote lines to associate each line with its corresponding Salesforce Product2 ID.

# In[199]:


#merge sf_parts with df_parts to get salesforce IDs for the parts
sf_parts.drop_duplicates(subset=['Name'], inplace=True) 
df_quote_lines = pd.merge(df_quote_lines, sf_parts, on='Name', how='inner')
df_quote_lines = df_quote_lines.rename(columns={'Id': 'inscor__Product__c'})
df_quote_lines.head()


# Selects only the required columns for the quote line insert and truncates the lead time field to comply with the 24-character Salesforce limit.

# In[200]:


df_quote_lines = df_quote_lines[['inscor__Customer_Reference_Line__c', 'inscor__Quantity_Requested__c', 'inscor__Lead_Time__c', 'inscor__Quote_Due__c', 'inscor__Product__c', 'inscor__Internal_Comments_rt__c']].copy()
#limit lead time to 24 characters
df_quote_lines['inscor__Lead_Time__c'] = df_quote_lines['inscor__Lead_Time__c'].astype(str).str[:24]
df_quote_lines.head()


# Inserts a new quote header record using the REST API and stores the returned ID. 

# In[ ]:


quote_header = {
    "inscor__Customer__c": "001ep000007jy2PAAQ", 
    "inscor__Reference__c": "UPLOAD",  
    "inscor__Contact__c": "003ep00000GNZaBAAX", 
    "Division__c": "Commercial",  
}
# Insert quote header using Salesforce REST API and store the new Id
quote_header_result = sf.inscor__Customer_Quote__c.create(quote_header)
quote_header_id = quote_header_result.get('id')
print(f"Inserted Quote Header Id: {quote_header_id}")  # Add the Quote Header Id to each line item

# its so easy to get all the record field back with the rest api in python just do a .get passing in the id


# Adds the quote header ID to each line item, inserts the quote lines using the Bulk API, and then prints summaries of successful and failed inserts along with any error messages returned by Salesforce.

# In[202]:


df_quote_lines['inscor__Customer_Quote__c'] = quote_header_id
records=df_quote_lines.to_dict(orient='records')
result = sf.bulk2.inscor__Customer_Quote_Line__c.insert(records=records, concurrency=100)
print(result)

#check the results
success, fail = get_bulk2_results(result)
print(f"Total records inserted: {len(success)}")
display(success.head(3))
print(f"Total records failed: {len(fail)}")
display(fail.head(3))

print(fail['sf__Error'])


# This pipeline transforms unstructured bid data into structured records, enriches and deduplicates parts, and inserts both quotes and line items into Salesforce using a mix of REST and Bulk APIs, showing a scalable and automation-friendly approach to external data integration.

# In[204]:


get_ipython().system('jupyter nbconvert --to html "03_real_data_pipeline.ipynb" --output-dir=../html')
get_ipython().system('jupyter nbconvert --to script "03_real_data_pipeline.ipynb" --output-dir=../notebook_script_exports')

