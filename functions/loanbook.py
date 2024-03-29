from datetime import datetime, timedelta
from google.cloud import bigquery

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}


def loanbook(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    id = request_json["id"]

    # authenticate request
    if checkuser(username) and checkbook(id) and checkloan(username, id) and checklimit(username):

        # connect to bigquery
        client = bigquery.Client()
        table_id = "bookit-297317.dataset.loans"
        table = client.get_table(table_id)

        # write to loans
        query = "INSERT INTO `bookit-297317.dataset.loans` (username,id,date) VALUES ('{}','{}', CURRENT_DATE())".format(username, id)
        query_job = client.query(query)
        results = query_job.result()

        return {'success': True}, 200

    elif not checkuser(username):
        return {'success': False, 'message': "Invalid Username"}, 400
    elif not checkbook(id):
        return {'success': False, 'message': "Invalid Book ID"}, 400
    elif not checkloan(username, id):
        return {'success': False, 'message': "Already Loaned"}, 400
    elif not checklimit(username):
        return {'success': False, 'message': "Maximum 5 Loans"}, 400
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


def checklimit(username):
    # connect to bigquery
    limitclient = bigquery.Client()
    table_id = "bookit-297317.dataset.loans"
    table = limitclient.get_table(table_id)

    # read from loans
    query = "SELECT * FROM `bookit-297317.dataset.loans` WHERE username='" + username + "'"
    query_job = limitclient.query(query)

    # get result
    results = query_job.result()
    if len(list(results)) < 5:
        return True
    else:
        return False