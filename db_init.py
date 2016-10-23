from pymongo import MongoClient
from hashlib import sha256
from datetime import datetime
from xmljson import badgerfish as bf
from bson.json_util import dumps
from xml.etree.ElementTree import fromstring


def get_db():
    client = MongoClient('localhost:27017')
    db = client['XML_SRV_TEST']
    return db

if __name__ == '__main__':
    db = get_db()
    users_col = db['users']
    organizations = db['organizations']

    # User collection initialization
    login = 'root'
    password = sha256('qwerty'.encode()).hexdigest()

    # Adding organization for testing purpose
    org_id = sha256('Test org 1'.encode()).hexdigest()
    test_org = {'org_id': org_id,
                'org_name': 'ТОВ ТЕСТ',
                'timestamp': datetime.now(),
                'doc_count': 0,
                'users': ['root',],
                'docs': []
                }

    users_col.insert_one({'login': login, 'password': password})
    organizations.insert(test_org)

    # Adding first document into test organization
    with open('test.xml', 'r') as file:
        xml_data = file.read()

    # First test doc
    json_data = dumps(bf.data(fromstring(xml_data)))
    document = {
        'doc_id': 1,
        'last_modified': datetime.now(),
        'data': json_data
    }

    result = organizations.update_one({'org_id': org_id}, {
        '$push': {
                "docs" : document
        }
    })

    print('DB init done.')