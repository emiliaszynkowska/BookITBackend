import datetime, json, jwt, flask
from google.cloud import bigquery
from aiohttp import web

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}


def login(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    password = request_json["password"]

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
        return authenticate(username, password)
    else:
        return {'success': False}, 400


def authenticate(username, password):
    # Generate token
    timeLimit = datetime.datetime.utcnow() + datetime.timedelta(minutes=10)
    payload = {"username": username, "exp": timeLimit}
    token = jwt.encode(payload, 'secretkey')
    send_data = {
        "error": "0",
        "message": "successful",
        "token": token.decode("UTF-8"),
        "elapsed_time": f"{timeLimit}"
    }
    return json_response({'token': token.decode('utf-8')})


def json_response(body='', **kwargs):
    kwargs['body'] = json.dumps(body or kwargs['body']).encode('utf-8')
    kwargs['content_type'] = 'text/json'
    return {'success': True, 'token': flask.jsonify(web.Response(**kwargs))}, 200
