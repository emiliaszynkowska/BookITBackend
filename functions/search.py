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


# Assuming a return is the opposite of a loan; will remove a loan instead of adding a return
def search(request):
    # get the request data
    request_json = request.get_json()
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

    # authenticate request
    check_token = gen_token(username)
    if True:  # check_token == token:

        # connect to bigquery
        client = bigquery.Client()
        table_id = "bookit-297317.dataset.loans"
        table = client.get_table(table_id)

        # remove from loans
        query = "SELECT * FROM `bookit-297317.dataset.books` WHERE Title='\"{}\"' OR Publisher='\"{}\"' OR Location='\"{}\"' OR ISSN='\"{}\"' OR eISSN='\"{}\"' OR ISBN='\"{}\"' OR Type='\"{}\"' OR Language='\"{}\"' OR Subjects='\"{}\"' OR ID='\"{}\"' OR URL='\"{}\"'".format(title, publisher, location, issn, eissn, isbn, type, language, subjects, id, url)
        query_job = client.query(query)
        results = query_job.result().to_dataframe().to_json(orient="records")

        return {'success': True, 'message': results}, 200

    else:
        return {'success': False, 'message': "Invalid Token"}, 400


def gen_token(username):
    payload = {
        'username': username,
        'exp': datetime.utcnow() + timedelta(seconds=JWT_EXP_DELTA_SECONDS)
    }

    jwt_token = jwt.encode(payload, JWT_SECRET, JWT_ALGORITHM)
    jwt_token = jwt_token.decode("utf-8")

    return jwt_token
