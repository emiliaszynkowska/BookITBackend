import jwt
from google.cloud import bigquery
from datetime import datetime, timedelta


headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}

JWT_SECRET = 'secret'
JWT_ALGORITHM = 'HS256'
JWT_EXP_DELTA_SECONDS = 86400


def overduebook(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    token = request_json["token"]
    dateNow = datetime.now().strftime('%Y-%m-%d')

    # authenticate request
    check_token = gen_token(username)
    if check_token == token and checkuser(username) and checkbook(id) and (checkloan(username, id)) == False:

        # connect to bigquery
        client = bigquery.Client()
        table_id = "bookit-297317.dataset.loans"
        table = client.get_table(table_id)

        query = "SELECT date FROM `bookit-297317.dataset.loans`"
        query_job = client.query(query)
        results = query_job.result()

        res_list = [x[0] for x in results]

        for x in res_list:
            if days_between(dateNow, x) > 30:
                counter = counter + 1
                query = "SELECT title FROM `bookit-297317.dataset.loan` LIMIT counter,1"
                query_job = client.query(query)
                results = query_job.result()
                print(results)
                continue
            counter = counter + 1;





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


def days_between(d1, d2):
  d1 = datetime.strptime(d1, "%Y-%m-%d")
  d2 = datetime.strptime(d2, "%Y-%m-%d")
  return abs((d2 - d1).days)


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


def gen_token(username):
    payload = {
        'username': username,
        'exp': datetime.utcnow() + timedelta(seconds=JWT_EXP_DELTA_SECONDS)
    }

    jwt_token = jwt.encode(payload, JWT_SECRET, JWT_ALGORITHM)
    jwt_token = jwt_token.decode("utf-8")

    return jwt_token