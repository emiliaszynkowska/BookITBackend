import jwt
from datetime import datetime, timedelta
from google.cloud import bigquery
from cerberus import Validator

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}

schema = {
    'username': {'type': 'string', 'required': True},
    'token': {'type': 'string', 'required': True},
    'title': {'type': 'string', 'required': True},
    'publisher': {'type': 'string', 'required': False},
    'location': {'type': 'string', 'required': False},
    'issn': {'type': 'string', 'required': False},
    'eissn': {'type': 'string', 'required': False},
    'isbn': {'type': 'string', 'required': False},
    'type': {'type': 'string', 'required': False},
    'language': {'type': 'string', 'required': False},
    'subjects': {'type': 'string', 'required': False},
    'id': {'type': 'string', 'required': True},
    'url': {'type': 'string', 'required': False}
}

JWT_SECRET = 'secret'
JWT_ALGORITHM = 'HS256'
JWT_EXP_DELTA_SECONDS = 86400


def addbook(request):
    # get the request data
    request_json = request.get_json()

    validator = Validator(schema)
    valid = validator.validate(request_json)

    if not valid:
        return "Invalid Request", 400

    username = request_json["username"]
    token = request_json["token"]
    title = request_json["title"]
    publisher = request_json["publisher"]
    location = request_json["location"]
    issn = request_json["issn"]
    eissn = request_json["eissn"]
    isbn = request_json["isbn"]
    type = request_json["type"]
    language = request_json["language"]
    subjects = request_json["subjects"]
    id = request_json["id"]
    url = request_json["url"]

    if gen_token(username) == token and checkbook(id):
        # connect to bigquery
        client = bigquery.Client()
        table_id = "bookit-297317.dataset.books"
        table = client.get_table(table_id)

        # write to books
        query = "INSERT INTO `bookit-297317.dataset.books` (Title, Publisher, Location, ISSN, eISSN, ISBN, Type, Language, Subjects, ID, URL) VALUES  ('\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\{}\"', '\"{}\"')".format(
            title, publisher, location, issn, eissn, isbn, type, language, subjects, id, url)

        # get result
        query_job = client.query(query)
        results = query_job.result()

        return {'success': True}, 200

    elif not checkbook(id):
        return {'success': False, 'message': "Invalid Book"}, 400
    else:
        return {'success': False, 'message': "Invalid Token"}, 400


def checkbook(id):
    # connect to bigquery
    bookclient = bigquery.Client()
    table_id = "bookit-297317.dataset.books"
    table = bookclient.get_table(table_id)

    # read from books
    query = "SELECT * FROM `bookit-297317.dataset.books` WHERE id='" + id + "'"
    query_job = bookclient.query(query)

    # get result
    results = query_job.result()
    if len(list(results)) == 0:
        return True
    else:
        return False


def gen_token(username):
    payload = {
        'username': username,
        'exp': datetime.utcnow() + timedelta(seconds=JWT_EXP_DELTA_SECONDS)
    }

    jwt_token = jwt.encode(payload, JWT_SECRET, JWT_ALGORITHM)
    jwt_token = jwt_token.decode("utf-8")

    return jwt_token
