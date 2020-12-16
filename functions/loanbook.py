from google.cloud import bigquery
import datetime

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}


def loanbook(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    id = request_json["id"]
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    # check request details
    if checkuser(username) and checkbook(id):
        # connect to bigquery
        client = bigquery.Client()
        table_id = "bookit-297317.dataset.loans"
        table = client.get_table(table_id)

        # write to loans
        query = "INSERT INTO `bookit-297317.dataset.loans` (username,id,date) VALUES ('" + username + "','" + id + "','" + date + "')"
        query_job = client.query(query)
        results = query_job.result()

        return {'success': True}, 200

    else:
        return {'success': False}, 400


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
