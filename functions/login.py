import json, jwt, flask
from datetime import datetime, timedelta
from google.cloud import bigquery

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}

JWT_SECRET = 'secret'
JWT_ALGORITHM = 'HS256'
JWT_EXP_DELTA_SECONDS = 86400


def login(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    password = request_json["password"]

    authenticated = authenticate(username, password)

    if not authenticated:
        return {'success': False, 'message': 'Invalid Credentials'}, 400

    token = gen_token(username)
    response_body = {'success': True, 'token': token}

    return json.dumps(response_body), 200


def authenticate(username, password):
    # connect to bigquery
    client = bigquery.Client()
    table_id = "bookit-297317.dataset.users"
    table = client.get_table(table_id)

    # read from users
    query = "SELECT * FROM `bookit-297317.dataset.users` WHERE username='" + username + "' AND password='" + password + "'"
    query_job = client.query(query)

    # get result
    results = query_job.result()
    if len(list(results)) > 0:
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
