import jwt, pandas, pyarrow
from datetime import datetime, timedelta
from google.cloud import bigquery

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}

JWT_SECRET = 'secret'
JWT_ALGORITHM = 'HS256'
JWT_EXP_DELTA_SECONDS = 86400


def search(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    entries = request_json["entries"]
    start = request_json["start"]
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

    # authenticate request
    check_token = gen_token(username)
    if checkuser(username) and isinstance(entries, int) and isinstance(start, int):  # check_token == token:

        # connect to bigquery
        client = bigquery.Client()
        table_id = "bookit-297317.dataset.loans"
        table = client.get_table(table_id)

        # remove from loans
        query = "SELECT * FROM `bookit-297317.dataset.books` WHERE Title LIKE \"{}\" OR Publisher LIKE \"{}\" OR Location LIKE \"{}\" OR ISSN LIKE \"{}\" OR eISSN LIKE \"{}\" OR ISBN LIKE \"{}\" OR Type LIKE \"{}\" OR Language LIKE \"{}\" OR Subjects LIKE \"{}\" OR ID=\"{}\" OR URL LIKE \"{}\"".format(
            title if title != "" else "null", publisher if publisher != "" else "null",
            location if location != "" else "null", issn if issn != "" else "null", eissn if eissn != "" else "null",
            isbn if isbn != "" else "null", type if type != "" else "null", language if language != "" else "null",
            subjects if subjects != "" else "null", id, url if url != "" else "null", entries, start)
        query_job = client.query(query)
        results = query_job.result().to_dataframe().to_json(orient="records")

        return {'success': True, 'message': results}, 200

    else:
        if not checkuser(username):
            return {'success': False, 'message': 'Invalid Username'}, 400
        elif not isinstance(entries, int):
            return {'success': False, 'message': 'Invalid Number Of Entries'}, 400
        elif not isinstance(start, int):
            return {'success': False, 'message': 'Invalid Start Index'}, 400


def checkuser(username):
    # connect to bigquery
    userclient = bigquery.Client()
    table_id = "bookit-297317.dataset.users"
    table = userclient.get_table(table_id)

    # read from users
    query = "SELECT * FROM `bookit-297317.dataset.users` WHERE username='" + username + "'"
    query_job = userclient.query(query)

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
