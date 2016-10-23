import json
import requests
from requests.auth import HTTPBasicAuth
import re

def post_doc(link, doc, user, password, default_encoding='utf-8'):
    pattern_encoding = re.compile(r'^<?.*encoding="([\w-]+)"', flags=re.IGNORECASE)
    fencoding = re.search(pattern_encoding, doc)

    try:
        fencoding = fencoding.group(1).lower()
    except:
        fencoding = default_encoding

    headers = {'Content-Type': 'application/xml', 'Accept-Charset': fencoding}
    r = requests.post(link, data.encode(fencoding), headers=headers, auth=HTTPBasicAuth(user, password))
    return r.text

def get_data(link, user, password):
    headers = {'Content-Type': 'application/xml'}
    r = requests.get(link, headers=headers, auth=HTTPBasicAuth(user, password))
    return r.text

def del_doc(link, user, password):
    headers = {'Content-Type': 'application/xml'}
    r = requests.delete(link, headers=headers, auth=HTTPBasicAuth(user, password))
    return r.text


with open('test4.xml', 'r', encoding='utf-8') as file:
    data = file.read()


# Using test DB
ORG_ID = '4dd6b7b15ca989e980886bf5881c0d3dad427a3d707bd0cb2ee67b4a86b535e4'
user = 'root'
password = 'qwerty'

# Get organization info
link = 'http://127.0.0.1:5000/api/v1.0/orgs/{0}'.format(ORG_ID)
response = get_data(link, user, password)
print(response)

# Insert document
link = 'http://127.0.0.1:5000/api/v1.0/docs/{0}'.format(ORG_ID)
response = json.loads(post_doc(link, data, user, password))
doc_id = response['doc_id']
print(response)

# Get document by ID
link = 'http://127.0.0.1:5000/api/v1.0/docs/{0}/{1}'.format(ORG_ID, doc_id)
response = get_data(link, user, password)
print(response)

# Delete document by ID
link = 'http://127.0.0.1:5000/api/v1.0/docs/{0}/{1}'.format(ORG_ID, doc_id)
response = del_doc(link, user, password)
print(response)
