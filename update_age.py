!pip install hubspot
!pip install --upgrade hubspot-api-client
!pip install google-cloud-secret-manager
import requests
import json
import urllib
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

"""retrieving API key from secretmanager"""
from google.colab import auth
auth.authenticate_user()
from google.cloud import secretmanager

def access_secret_version(project_id, secret_id, version_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    return payload

"""HAPI_KEY is API KEY"""
HAPI_KEY = access_secret_version("name of Virtual Machine", "API KEY", "1")

"""getting the currect number of contacts existing in hubspot."""
"""the documentation for below codes ----> https://legacydocs.hubspot.com/docs/methods/contacts/get-contacts-statistics"""

parameters = {"includeTotalContacts" : True,'hapikey' : HAPI_KEY}
url = "https://api.hubapi.com/contacts/v1/contacts/statistics?" 
parameters = urllib.parse.urlencode(parameters)
get_url = url + parameters
headers = {}
r = requests.get(url= get_url, headers = headers)
response_dict = json.loads(r.text)

"""num_row is the number of all contacts data"""
num_row = response_dict["contacts"]

num_row = 300

count = 100
contact_list = []
"""sending http request to get all contacts information""" 
"""property included in response is only age and date_of_birth"""
"""The documentation for below codes ----> https://legacydocs.hubspot.com/docs/methods/contacts/get_contacts"""

get_all_contacts_url = "https://api.hubapi.com/contacts/v1/lists/all/contacts/all?property=age&property=date_of_birth&"
parameter_dict = {'hapikey': HAPI_KEY, 'count': count}
headers = {}
# Paginate your request using offset
has_more = True
while has_more:
	parameters = urllib.parse.urlencode(parameter_dict)
	get_url = get_all_contacts_url + parameters
	r = requests.get(url= get_url, headers = headers)
	response_dict = json.loads(r.text)
	has_more = response_dict['has-more']
	contact_list.extend(response_dict['contacts'])
	parameter_dict['vidOffset']= response_dict['vid-offset']
	if len(contact_list) >= num_row : # Exit pagination, based on num_row. 
		print('maximum number of results exceeded')
		break
print('loop finished')

list_length = len(contact_list) 

print("You've succesfully parsed through {} contact records and added them to a list".format(list_length))

parameter_dict['vidOffset']

response_dict['status']

r.text

r.status_code != 204

"""extracting only needed information for updating age column"""
m_list = []
for i in range(len(contact_list)):
  dict1 = contact_list[i]["properties"]
  dict2 = {"value":contact_list[i]["vid"]}
  dict1["contact_id"] = dict2
  m_list.append(dict1)

"""Create data frame having date_of_birth, age columns, and lastmodifieddate columns came from dict1 and contact_id came from dict2"""
cols = ['age','contact_id','date_of_birth','lastmodifieddate']
df = pd.DataFrame(m_list,columns = cols)

import numpy as np
def remove(x):
  """
    This function simply extract value from dictionary

    Parameters
    ----------
    x : dict

    Returns
    -------
    string(value of dict)
        The value of dictionary

    Examples
    --------
    >>> revmove({"value":"06/22/2000"})
    "06/22/2000"

    >>> Nan
    Nan
    
    """
  if type(x) == dict:
    x = x["value"]
  return x

df = df.applymap(remove)

"""age column contains string data like '19,' so change data type to float."""
df["age"] = pd.to_numeric(df['age'])

import datetime
"""separate dataframe into two dataframes
   df_2 have incorrect format of date_of_birth which is MM/DD/YY. it should be corrected to MM/DD/YYYY"""

df_3 = df[df['date_of_birth'].str.fullmatch(r'\d{1,2}/\d{1,2}/\d{4}')== True]
df_2 = df[df['date_of_birth'].str.fullmatch(r'\d{1,2}/\d{1,2}/\d{2}')== True]

def func1(x):
  
  """
    This function transform add two digits the front of the input data
    I needed the date_of_birth data which has MM/DD/YYYY shape but
    some of data had different shape which is MM/DD/YY so I added 19 or 20 to them.

    Parameters
    ----------
    x : string
    x shows year

    Returns
    -------
    string
       
    See Also
    --------
    subtract : Subtract one integer from another.

    Examples
    --------
    >>> func1('00')
    '2000'
    >>> func1('98')
    '1998'
    
    """
  if (x[-2:] == "00" or x[-2:] =="01" or x[-2:] =="02" or x[-2:] =="03" or x[-2:] =="04"):
      y = x[:-2] + "20" + x[-2:]
  else:
     y = x[:-2] + "19" + x[-2:]
  return y

df_2["date_of_birth"] = df_2["date_of_birth"].apply(func1)
df_4 = pd.concat([df_3, df_2])
df_4["date_of_birth"] = pd.to_datetime(df_4["date_of_birth"], format='%m/%d/%Y', errors='coerce')
df_4 = df_4[pd.notnull(df_4['date_of_birth'])]

def agee(x):
    """
    This function calculate age from date_of_birth data

    Parameters
    ----------
    x : datetime

    Returns
    -------
    int
        The age calculated from x

    Examples
    --------
    >>> agee('06/22/2000')
    22
    >>> agee('07/02/1980)
    42
    """
    today = datetime.date.today()
    birthday = x
    return (int(today.strftime("%Y%m%d")) - int(birthday.strftime("%Y%m%d"))) // 10000

df_4["age"] = df_4["date_of_birth"].apply(agee)
df_4 = df_4[["contact_id","age"]]
df = df[["contact_id", "age"]]
df_inner = df.merge(df_4, how = 'inner' ,indicator=False)
df_diff = df_4[df_4['contact_id'].isin(df_inner["contact_id"].to_list()) == False]

df_diff

"""update age information by hubspot API
   the sample code that I used is ---->https://developers.hubspot.com/docs/api/crm/contacts"""
  
import hubspot
from pprint import pprint
from hubspot.crm.contacts import SimplePublicObjectInput, ApiException

client = hubspot.Client.create(api_key=HAPI_KEY)
"""only age property is updated below code"""

for yo,id in zip(df_diff["age"],df_diff["contact_id"]):
  properties = {
    "age": yo
  }
  simple_public_object_input = SimplePublicObjectInput(properties=properties)
  try:
      api_response = client.crm.contacts.basic_api.update(contact_id=id, simple_public_object_input=simple_public_object_input)
      pprint(api_response)
  except ApiException as e:
      print("Exception when calling basic_api->update: %s\n" % e)
