from datetime import datetime, timedelta
from google.cloud import bigquery

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}


def changeusername(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    password = request_json["password"]
    newUsername = request_json["newusername"]

    # authenticate request
    if checkuser(username) and checkpassword(password):

        # connect to bigquery for users
        client = bigquery.Client()
        table_id = "bookit-297317.dataset.users"
        table = client.get_table(table_id)

        # check validity of new username
        query = "SELECT * FROM `bookit-297317.dataset.users` WHERE username='" + newUsername + "'"
        query_job = client.query(query)
        results = query_job.result()

        if len(list(results)) > 0:
            return {'success': False, 'message': "This username is already taken!"}, 400
        else:
            # connect to bigquery for users
            client = bigquery.Client()
            table_id = "bookit-297317.dataset.users"
            table = client.get_table(table_id)

            # update name
            query = "UPDATE `bookit-297317.dataset.users` SET username='" + newUsername + "' WHERE username='" + username + "'"
            query_job = client.query(query)
            results = query_job.result()

            # connect to bigquery for loans
            client = bigquery.Client()
            table_id = "bookit-297317.dataset.loans"
            table = client.get_table(table_id)

            # update name
            query = "UPDATE `bookit-297317.dataset.loans` SET username='" + newUsername + "' WHERE username='" + username + "'"
            query_job = client.query(query)
            results = query_job.result()

            # connect to bigquery for overdue books
            client = bigquery.Client()
            table_id = "bookit-297317.dataset.overduebooks"
            table = client.get_table(table_id)

            # update name
            query = "UPDATE `bookit-297317.dataset.overduebooks` SET username='" + newUsername + "' WHERE username='" + username + "'"
            query_job = client.query(query)
            results = query_job.result()

            return {'success': True}, 200

        return {'success': True}, 200

    elif not checkuser(username):
        return {'success': False, 'message': "Invalid Username"}, 400
    elif not checkpassword(password):
        return {'success': False, 'message': "Invalid Password"}, 400
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


def checkpassword(password):
    # connect to bigquery
    userclient = bigquery.Client()
    table_id = "bookit-297317.dataset.users"
    table = userclient.get_table(table_id)

    # read from users
    query = "SELECT * FROM `bookit-297317.dataset.users` WHERE password='" + password + "'"
    query_job = userclient.query(query)

    # get result
    results = query_job.result()
    if len(list(results)) > 0:
        return True
    else:
        return False