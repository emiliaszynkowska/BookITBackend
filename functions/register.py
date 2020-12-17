from google.cloud import bigquery

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}


def register(request):
    # get the request data
    request_json = request.get_json()
    username = request_json["username"]
    password = request_json["password"]

    # connect to bigquery
    client = bigquery.Client()
    table_id = "bookit-297317.dataset.users"
    table = client.get_table(table_id)

    # write to users
    query = "INSERT INTO `bookit-297317.dataset.users` (username,password) VALUES ('" + username + "','" + password + "')"
    query_job = client.query(query)
    results = query_job.result()

    return {'success': True}, 200


