from google.cloud import bigquery

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
        return "Login successful."
    else:
        return "Login unsuccessful."


