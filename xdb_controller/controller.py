from functools import wraps
from pymongo import MongoClient
import pymongo.errors as db_errors
from hashlib import sha256
from datetime import datetime
from xmljson import badgerfish as bf
from bson.son import SON
from bson.json_util import dumps, loads
from bson.objectid import ObjectId

try:
    from lxml.etree import Element, fromstring, tostring, ParseError
    import lxml.etree as ET
    from io import BytesIO
except:
    from xml.etree.ElementTree import Element, ElementTree, fromstring, tostring, ParseError
    from xml.dom import minidom

import uuid

DEBUG = False

def user_validate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        driver = args[0]
        user = args[1]
        org_id = args[2]

        res = driver.org_check_user(org_id, user)
        if res:
            return func(*args, **kwargs)
        else:
            return {'result': 0}
    return wrapper

# Magic: getting arguments names of given function
def __get_func_args_names(func):
    # func_const_count - counts all function constants minus one (first always(?) none)
    # func_default_args - get only function arguments not all function variables
    func_const_count = len(func.__code__.co_consts)-1
    func_default_args = func.__code__.co_varnames[:func_const_count+1]
    return func_default_args

def debug_handler(func):
    @wraps(func)
    def wrapper(inst, *args, **kwargs):
        try:
            result = func(inst, *args, **kwargs)
        except db_errors.DuplicateKeyError as err:
            func_default_args = __get_func_args_names(func)
            error_msg = '\n[DEBUG_START]\n' \
                        'Insert/update operation failed due to key error. ' \
                        'Document with such _id already presence in DB.\n' \
                        'Class name: {0}\n' \
                        'Method name: {1}{2}\n' \
                        'Args: {3}\n' \
                        'Kwargs: {4}\n' \
                        '[DEBUG_END]\n'.format(str(inst.__class__.__name__),
                                                                          str(func.__name__),
                                                                          func_default_args,
                                                                          args,
                                                                          kwargs)
            if DEBUG:
                print(error_msg)

            prt = True
            if prt:
                raise err
        except Exception as err:
            func_default_args = __get_func_args_names(func)
            error_msg = '\n[DEBUG_START]\n' \
                        'Unknown exception.' \
                        'Class name: {0}\n' \
                        'Method name: {1}{2}\n' \
                        'Args: {3}\n' \
                        'Kwargs: {4}\n' \
                        '[DEBUG_END]\n'.format(str(inst.__class__.__name__),
                                                                          str(func.__name__),
                                                                          func_default_args,
                                                                          args,
                                                                          kwargs)
            if DEBUG:
                print(error_msg)

            prt = True
            if prt:
                raise err

        return result
    return wrapper

class DBConnection:
    # Make sure we always have the only working connection at ones
    __connection = None

    def __init__(self, host='localhost', port='27017'):
        self.host = '{}:{}'.format(host, str(port))
        self.mclient = self.__make_connection(self.host)

    @classmethod
    def __make_connection(cls, host):
        if not cls.__connection:
            cls.__connection = MongoClient(host)
        return cls.__connection

    # returns db client object
    def __call__(self, db_name):
        return self.mclient[db_name]


# DB model
class Driver:

    def __init__(self, db_name, collection_name, root_user, *args, **kwargs):
        self.db_name = db_name
        self.collection_name = collection_name
        self.__args = args
        self.__kwargs = kwargs

        assert isinstance(root_user, UserModel), 'user should be created via UserModel instance'
        self.__root_user = root_user

    def connect(self):
        conn = DBConnection(*self.__args, **self.__kwargs)
        self.db = conn(self.db_name)

    def _init_users_storage(self):
        if not self.__collection_check_exists('users'):
            coll = self.db['users']
            result = coll.insert_one(self.__root_user.to_dict())

            if result.inserted_id:
                return {'result': 1}
            return {'result': 0}
        return {'result': 0}

    def db_user_add(self, user):
        if isinstance(user, (tuple, list)):
            assert len(user) == 2, '"user" should be tuple that contains exact two strings' \
                                   ' - login and password. This contains %d elements' % len(user)

            user = UserModel(*user)

        if not isinstance(user, UserModel):
            raise TypeError

        if self.db_user_check_exists(user):
            return {'result': 0}
        else:
            coll = self.db['users']
            result = coll.insert_one(user.to_dict())

            if result.inserted_id:
                return {'result': 1}
            return {'result': 0}

    def db_user_remove(self, user):
        if isinstance(user, (tuple, list)):
            assert len(user) == 2, '"user" should be tuple that contains exact two strings' \
                                   ' - login and password. This contains %d elements' % len(user)

            user = UserModel(*user)

        if not isinstance(user, UserModel):
            raise TypeError

        if not self.db_user_check_exists(user):
            return {'result': 0}
        else:
            coll = self.db['users']
            result = coll.delete_one({'login': user.login})

            if result.deleted_count > 0:
                return {'result': 1, 'login': user.login}
            return {'result': 0}

    def db_user_check_password(self, user):
        if isinstance(user, (tuple, list)):
            assert len(user) == 2, '"user" should be tuple that contains exact two strings' \
                                   ' - login and password. This contains %d elements' % len(user)

            user = UserModel(*user)

        if not isinstance(user, UserModel):
            raise TypeError

        if self.db_user_check_exists(user):
            coll = self.db['users']
            cur = coll.find_one({'login': {'$eq': user.login}, 'password': {'$eq': user.password}},
                                {'password': 0,
                                 '_id': 0})
            if cur:
                return user
        return False

    def db_user_check_exists(self, user):
        if isinstance(user, (tuple, list)):
            assert len(user) == 2, '"user" should be tuple that contains exact two strings' \
                                   ' - login and password. This contains %d elements' % len(user)

            user = UserModel(*user)

        if not isinstance(user, UserModel):
            raise TypeError

        coll = self.db['users']
        cur = coll.find_one({'login': user.login}, {'login': 1, '_id': 0})
        if cur:
            return True
        return False

    @debug_handler
    def org_create_one(self, *args, **kwargs):
        org = OrganizationModel(*args, **kwargs)
        # Set default root user for organization
        org = self.__set_root_user([org])[0]
        org_data = org.toDict()

        coll = self.db[self.collection_name]
        result = coll.insert_one(org_data)
        if result.inserted_id:
            return {'result': 1, org.org_name: org.org_id}
        return {'result': 0}

    @debug_handler
    def org_create_many(self, org_list, *args, **kwargs):
        assert isinstance(org_list, (list, dict,)), \
            'org_list should be of list or dict type. Given type %s' % str(type(org_list))

        orgs = []
        output = {'result': 0}

        # org_list is a list of..
        if isinstance(org_list, list):
            # OrganizationModel instances
            if all(isinstance(org, OrganizationModel) for org in org_list):
                for org in org_list:
                    orgs.append(org.toDict())
            # formed dicts
            elif all(isinstance(org, dict) and 'org_id' in org.keys() for org in org_list):
                orgs.extend(org_list)
            # unformed dicts
            elif all(isinstance(org, dict) and 'org_name' in org.keys() for org in org_list):
                for org in org_list:
                    org_obj = OrganizationModel(org['org_name'], *args, **kwargs)
                    orgs.append(org_obj.toDict())
            # unknown staff
            else:
                pass
        # org_list is a dict of..
        elif isinstance(org_list, dict):
            for key in org_list.keys():
                # formed dicts
                if org_list[key]['org_id']:
                    orgs.append(org_list[key])
                # unformed dicts
                elif org_list[key]['org_name']:
                    org_obj = OrganizationModel(org_list[key]['org_name'], *args, **kwargs)
                    orgs.append(org_obj.toDict())
                # unknown staff
                else:
                    pass
        else: raise TypeError('org_list should contains only OrganizationModel instances')

        if len(orgs) == 0:
            return output

        # Setting default root user for all documents in main collection
        orgs = self.__set_root_user(orgs)

        coll = self.db[self.collection_name]
        result = coll.insert_many(orgs).inserted_ids
        for oid in result:
            specified_fields = { 'org_id': 1,
                                 'org_name': 1,
                                 '_id': 0}
            org_data = self.__find_by_id(oid, specified_fields)
            output[org_data['org_name']] = org_data['org_id']
        output['result'] = 1
        return output

    def org_add_user(self, org_id, user):
        if isinstance(user, (tuple, list)):
            assert len(user) == 2, '"user" should be tuple that contains exact two strings' \
                                   ' - login and password. This contains %d elements' % len(user)

            user = UserModel(*user)

        if not isinstance(user, UserModel):
            raise TypeError

        assert user.login != 'root', 'Login "root" usage restriction.'

        if not self.org_check_user(org_id, user):
            coll = self.db[self.collection_name]
            result = coll.update_one({'org_id': org_id}, {'$push': {'users': user.login}})

            if result.modified_count == 0:
                return {'result': 0}
            return {'result': 1}
        return {'result': 0}

    def org_remove_user(self, org_id, user):
        if isinstance(user, (tuple, list)):
            assert len(user) == 2, '"user" should be tuple that contains exact two strings' \
                                   ' - login and password. This contains %d elements' % len(user)

            user = UserModel(*user)

        if not isinstance(user, UserModel):
            raise TypeError

        assert user.login != 'root', 'Login "root" usage restriction.'

        if self.org_check_user(org_id, user):
            coll = self.db[self.collection_name]
            result = coll.update_one({'org_id': org_id}, {'$pull': {'users': user.login}})

            if result.modified_count == 0:
                return {'result': 0}
            return {'result': 1}
        return {'result': 0}

    def org_check_user(self, org_id, user):
        if isinstance(user, (tuple, list)):
            assert len(user) == 2, '"user" should be tuple that contains exact two strings' \
                                   ' - login and password. This contains %d elements' % len(user)

        coll = self.db[self.collection_name]
        cur = coll.find_one({'org_id': org_id, 'users': user}, {'users': 1, '_id': 0})

        if cur:
            return True
        return False

    def org_get_info(self, org_id, exclude_fields={}):
        exclude_fields['users'] = 0
        exclude_fields['docs'] = 0
        exclude_fields['_id'] = 0

        cur = self.db[self.collection_name].find_one({'org_id': org_id},
                                    exclude_fields)
        return cur

    @user_validate
    def doc_create_one(self, user, org_id, data, encoding='utf-8'):
        doc_id = self.org_get_info(org_id)['doc_count'] + 1

        doc = DocumentModel(doc_id)
        try:
            doc.data = data
        except ParseError as err:
            return {'result': 0, 'error': 'Document data corrupted. Unable to parse.'}
        doc.encoding = encoding

        coll = self.db[self.collection_name]
        result = coll.update_one({'org_id': org_id},
                                                  {'$inc': {'doc_count': 1},
                                                   '$push': {'docs': doc.to_dict()}})
        if result.matched_count == 0:
            return {'result': 0}
        if result.modified_count != 0:
            return {'result': 1, 'doc_id': doc_id}
        return {'result': 0}

    @debug_handler
    def doc_create_many(self, org_id, data_list, encoding='utf-8'):
        result = None
        start_id = self.org_get_info(org_id)['doc_count']
        doc_id = start_id + 1

        if all(isinstance(doc, DocumentModel) for doc in data_list):
            if all([doc.data for doc in data_list]):
                bulk = self.db[self.collection_name].initialize_ordered_bulk_op()
                organization = bulk.find({'org_id': org_id})
                for doc in data_list:
                    doc.doc_id = doc_id
                    doc.encoding = encoding
                    organization.update({'$inc': {'doc_count': 1},
                                        '$push': {'docs': doc.to_dict()}})
                    doc_id += 1
                result = bulk.execute()

        elif all(isinstance(doc, dict) for doc in data_list):
            bulk = self.db[self.collection_name].initialize_ordered_bulk_op()
            organization = bulk.find({'org_id': org_id})
            for doc in data_list:
                # the reason of using DocModel instance instead give dictionary - validation in DocModel
                doc = DocumentModel.from_dict(doc)
                doc.doc_id = doc_id
                doc.encoding = encoding
                organization.update({'$inc': {'doc_count': 1},
                                        '$push': {'docs': doc.to_dict()}})
                doc_id += 1
            result = bulk.execute()
        else:
            pass
        if not result:
            return {'result': 0}
        if result['nModified'] != len(data_list):
            return {'result': 0}
        else:
            return {'result': 1, 'doc_first_id': start_id+1, 'doc_last_id': doc_id-1}

    @user_validate
    def doc_find_one(self, user, org_id, doc_id):
        coll = self.db[self.collection_name]
        cur = coll.find({'org_id': org_id, 'docs.doc_id': doc_id}, {'docs': 1, '_id': 0})

        for element in cur:
            for doc in element['docs']:
                if doc['doc_id'] == doc_id:
                    return doc

    def doc_remove_one(self, org_id, doc_id):
        coll = self.db[self.collection_name]
        result = coll.update_one({'org_id': org_id}, {'$pull': {'docs': {'doc_id': doc_id}}})
        if result.modified_count > 0:
            return {'result': 1, 'doc_id': doc_id}
        return {'result': 0}

    def __collection_check_exists(self, coll_name):
        coll = self.db[coll_name]
        if coll.count() == 0: # -> collection is empty_so_doesnt_exists
            return False
        return True

    def __find_by_id(self, object_id, specified_fields={}):
        assert isinstance(object_id, ObjectId)
        cur = self.db[self.collection_name].find_one({'_id': object_id},
                                    specified_fields)
        return cur

    def __set_root_user(self, org_list):
        assert isinstance(org_list, list), 'org_list should be of list type, not %s' % type(org_list)

        output = []
        for org in org_list:
            if isinstance(org, OrganizationModel):
                if str(self.__root_user) not in org.users:
                    org.add_user(self.__root_user)
            elif isinstance(org, dict):
                if str(self.__root_user) not in org['users']:
                    users = org['users']
                    users.extend([str(self.__root_user)])
                    org['users'] = users
            output.append(org)
        return output


# Organization model
class OrganizationModel:

    def __init__(self, org_name, users=[], docs=[]):
        self.org_id = uuid.uuid4().hex
        assert isinstance(org_name, str), 'org_name should be of string type. Only alpha-numerical symbols allowed'
        assert len(org_name) > 0 and len(org_name) < 150, \
            'organization name must have at least one letter but no more that 150. You pass: %d letters' % len(org_name)
        self.org_name = org_name
        self.creation_date = datetime.now()
        self.doc_count = 0

        assert all(isinstance(user, UserModel) for user in users) if len(users) > 0 else True, \
            'users list should consists of UserModel objects or be empty'
        self.users = users

        assert all(isinstance(doc, DocumentModel) for doc in docs) if len(docs) > 0 else True, \
            'documents list should consists of DocumentModel objects or be empty'
        self.docs = docs

    def add_user(self, user):
        assert isinstance(user, UserModel), 'user should be created via UserModel instance'
        self.users.extend([str(user)])
        return True

    def remove_user(self, user):
        assert isinstance(user, UserModel), 'user should be created via UserModel instance'
        self.users.remove(str(user))
        return True

    def toDict(self):
        json_data = {
            'org_id': self.org_id,
            'org_name': self.org_name,
            'creation_date': datetime.now(),
            'doc_count': 0,
            'users': self.users,
            'docs': self.docs
        }
        return json_data

    @classmethod
    def fromDict(cls, dic):
        assert isinstance(dic, dict), 'dic should be dict type, not %s' % type(dic)
        try:
            users = dic['users']
        except KeyError:
            users = []

        try:
            docs = dic['docs']
        except KeyError:
            docs = []

        return cls(dic['org_name'], users, docs)

# Document model
class DocumentModel:

    def __init__(self, doc_id=0, encoding='utf-8', *args, **kwargs):
        self._data = None
        self.doc_id = doc_id
        self.encoding = encoding
        self.timestamp = datetime.now()

    def to_dict(self):
        assert self.data, 'need to load data first before forming dict'

        document = {
                'doc_id': self.doc_id,
                'last_modified': self.timestamp,
                'encoding': self.encoding,
                'data': self.data
        }
        return document

    def to_xml(self):
        return self.json_to_xml(self._data)

    @classmethod
    def from_dict(cls, dic):
        assert isinstance(dic, dict), 'should be of dict type, not %s' % type(dic)

        if 'data' in dic.keys():
            document = cls()
            document.data = dic['data']
            if 'encoding' in dic.keys():
                document.encoding = dic['encoding']
        else:
            raise KeyError('No "data" key found for instancing DocumentModel')
        return document

    @classmethod
    def xml_to_json(cls, doc):

        '''
        Converts XML to JSON string
        '''
        try:
            tree = fromstring(doc)
            data = dumps(bf.data(tree))
        except TypeError as err:
            print('ERROR: doc should be of string type, not %s' % type(doc))
            raise
        except ParseError as err:
            print('ERROR: xml file structure is corrupted')
            raise
        except Exception as err:
            print('ERROR: unknown error')
            raise
        return data

    @classmethod
    def json_to_xml(cls, doc, default_root_name='root', method='xml', encoding='utf-8', prettify=True):
        assert any([True if f in method else False for f in ['html', 'xml', 'text', 'c14n']]), \
            'format argument should be one of html/xml/text only!'

        shorty_elements = True if prettify else False

        try:
            if isinstance(doc, str):
                json_dict = loads(doc, object_pairs_hook=SON) # <- preserves order in dict
            elif isinstance(doc, (dict, SON)):
                json_dict = doc

            # Forming root element with namespace(s) if present;
            # Cleaning JSON document before creating XML
            if len(json_dict.keys()) == 1:
                for root_element in json_dict.keys():
                    continue

                attribs = []
                text = None
                uri = None

                for key in json_dict[root_element].keys():
                    if '@' in key:
                        if key == '@xmlns':
                            ns_counter = 0
                            NS_MAP = dict()
                            for prefix in json_dict[root_element][key].keys():
                                if prefix != '$':
                                    uri = json_dict[root_element][key][prefix]
                                else:
                                    uri = json_dict[root_element][key][prefix]
                                    prefix = 'ns%d' % ns_counter
                                    ns_counter += 1
                                NS_MAP[prefix] = uri
                        else:
                            attribs.append((key, json_dict[root_element][key]))
                    elif '$' in key:
                        text = json_dict[root_element][key]

                if uri:
                    root = Element('{0}{1}'.format('{' + uri + '}', root_element), nsmap=NS_MAP)
                else:
                    root = Element(root_element)

                for attr in attribs:
                    root.set(str(attr[0].lstrip('@')), str(attr[1]))
                    del json_dict[root_element][attr[0]]
                root.text = text
                if text: del json_dict[root_element]['$']

                # Remove root key from JSON. Its value would become subelement(s) of created XML
                body = json_dict.pop(root_element)
            else:
                root = Element(default_root_name)

            xml_tree = bf.etree(body, root=root)

            if isinstance(root, ET._Element):
                tree = xml_tree.getroottree()
                file_object = BytesIO()

                if method.lower() == 'c14n':
                    tree.write(file_object, method='c14n', pretty_print=prettify)
                else:
                    tree.write(file_object, encoding=encoding, method=method, xml_declaration=True, pretty_print=prettify)

                # The XML declaration, including version number and character encoding is omitted from the canonical form.
                # https://www.w3.org/TR/xml-c14n#NoXMLDecl
            else:
                tree = ElementTree(xml_tree)
                file_object = BytesIO()

                if method.lower() == 'c14n':
                    tree.write(file_object, encoding=encoding, method='xml', xml_declaration=False, short_empty_elements=shorty_elements)
                else:
                    tree.write(file_object, encoding=encoding, method=method, xml_declaration=True, short_empty_elements=shorty_elements)

            data = file_object.getvalue().decode(encoding)

        except TypeError as err:
            raise
        except ValueError as err:
            print('ERROR: conversion error. Expected json/dict - like doc type.')
            raise
        except ParseError as err:
            print('ERROR: xml file structure is corrupted')
            raise
        except Exception as err:
            print('ERROR: unknown error')
            raise
        return data

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        if not self.__json_validator(data):
            self._data = self.xml_to_json(data)
        else:
            self._data = data

    def __json_validator(self, doc):
      try:
        json_object = loads(doc, object_pairs_hook=SON) # <- preserves order in dict
      except ValueError as err:
        return False
      return True

    def __str__(self):
        return self.json_to_xml(self._data)

# User model
class UserModel:

        def __init__(self, login, password):
            assert isinstance(login, str), 'login should be of string type, not %s' % type(login)
            assert len(login) >= 4 and len(login) < 40, \
                'organization name must have at least 4 letters but no more that 40. You pass: %d letters' % len(login)
            super().__setattr__('password', self.__save_password(password))
            super().__setattr__('login', login.lower())
            super().__setattr__('id', self.login)

        def to_dict(self):
            return {'login': self.login, 'password': self.password}

        def __save_password(self, password):
            return sha256(password.encode()).hexdigest()

        # Makes UserModel instance immutable after initialization
        # this prevents any changes (e.g. in login or password) after class instantiation
        def __setattr__(self, name, val):
            raise ValueError("User is immutable after initialization")

        def __str__(self):
            return self.login

        def __eq__(self, other):
            if self.login == other.login:
                return self.password == other.password
            return False