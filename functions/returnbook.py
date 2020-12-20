import jwt
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
def returnbook(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    id = request_json["id"]
    token = request_json["token"]
    date = datetime.now().strftime('%Y-%m-%d')

    # authenticate request
    check_token = gen_token(username)
    if check_token == token and checkuser(username) and checkbook(id) and (checkloan(username, id) == False):

        # connect to bigquery
        client = bigquery.Client()
        table_id = "bookit-297317.dataset.loans"
        table = client.get_table(table_id)

        # remove from loans
        query = "DELETE FROM `bookit-297317.dataset.loans` (username,id,date) VALUES ('\"{}\"','\"{}\"','\"{}\"')".format(username, id, date)
        query_job = client.query(query)
        results = query_job.result()

        return {'success': True}, 200

    elif not checkuser(username):
        return {'success': False, 'message': "Invalid Username"}, 400
    elif checkloan(username, id):
        return {'success': True, 'message': "Book hasn't been loaned"}, 400
    elif not checkbook(id):
        return {'success': False, 'message': "Invalid Book ID"}, 400
    else:
        return {'success': False, 'message': "Invalid Token"}, 400


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

def checkloan(username,id):
    # connect to bigquery
    loanclient = bigquery.Client()
    table_id = "bookit-297317.dataset.loans"
    table = loanclient.get_table(table_id)

    # read from loans
    query = "SELECT * FROM `bookit-297317.dataset.loans` WHERE username='" + username + "' AND id='" + id + "'"
    query_job = loanclient.query(query)

    # get result
    results = query_job.result()
    if len(list(results)) == 0:
        return True
    else:
        return False