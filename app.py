import base64
from bson.json_util import dumps
from flask import Flask, jsonify, abort, make_response, request
from flask_login import LoginManager, login_required, current_user
from xdb_controller.controller import Driver, UserModel, DocumentModel

app = Flask(__name__)
# DB name
DB_NAME = 'XML_SRV_TEST'
app.config['MONGO1_DBNAME'] = DB_NAME

login_manager = LoginManager()
login_manager.init_app(app)

# DB connection setup
user = UserModel('root', 'qwerty')
driver = Driver(DB_NAME, 'organizations', user)
driver.connect()

class FlaskUser(UserModel):

    @classmethod
    def convert(cls, login, password):
        return cls(login, password)

    # def __save_password(self, password):
    #     return password

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id

@app.errorhandler(404)
def not_found(error):
    return make_response(dumps({'error': 'Not found'}), 404)


@login_manager.request_loader
def login_basic_auth(request):
    api_key = request.headers.get('Authorization')
    if api_key:
        api_key = api_key.replace('Basic ', '', 1)
        try:
            login, password = base64.b64decode(api_key).decode('utf-8').split(':')

            # flask_user needed for Flask auth decorators
            flask_user = FlaskUser.convert(login, password)
            if flask_user:
                user = driver.db_user_check_password(flask_user) # return False if wrong user/pass
                if user:
                    return flask_user
            else:
                return None
        except TypeError:
            pass
    return None

@app.route('/', methods=['GET'])
@login_required
def home_page():
    user = current_user.get_id()
    return jsonify({'user': user})

@app.route('/api/v1.0/orgs/<string:org_id>', methods=['GET'])
def get_org_info(org_id):

    org = driver.org_get_info(org_id)

    if not org:
        abort(404)
    return make_response(jsonify(org))

@app.route('/api/v1.0/docs/<string:org_id>/<int:doc_id>', methods=['GET'])
@login_required
def get_doc(org_id, doc_id):

    user = current_user.get_id()
    doc = driver.doc_find_one(user, org_id, doc_id)

    if not doc:
        abort(404)

    try:
        encoding = doc['encoding']
    except:
        encoding = 'utf-8'

    xml = DocumentModel.json_to_xml(doc['data'], method='xml', encoding=encoding)

    resp = make_response(xml)
    resp.headers['Content-Type'] = 'text/xml; charset={}'.format(doc['encoding'])
    return resp

@app.route('/api/v1.0/docs/<string:org_id>', methods=['POST'])
@login_required
def add_doc(org_id):
    result = {'result': 0}
    if request.content_type == 'application/xml':
        if request.accept_charsets:
            raw_data = request.data
            data = raw_data.decode(request.headers['Accept-Charset'])
            user = current_user.get_id()
            result = driver.doc_create_one(user, org_id, data)
    else:
        result = {'result': 0, 'error': 'No XML data received.'}
    return jsonify(result)

@app.route('/api/v1.0/docs/<string:org_id>/<int:doc_id>', methods=['DELETE'])
@login_required
def delete_doc(org_id, doc_id):
    doc = driver.doc_remove_one(org_id, doc_id)

    if doc:
        return jsonify({'resul': 1, 'doc_id': doc_id})
    return jsonify({'result': 0, 'error': 'No document with ID %s found.' % doc_id})

if __name__ == '__main__':
    app.run(debug = True)