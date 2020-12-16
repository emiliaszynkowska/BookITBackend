from google.cloud import bigquery
from cerberus import Validator

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}

schema = {
    'title': {'type': 'string', 'required': True},
    'publisher': {'type': 'string', 'required': True},
    'location': {'type': 'string', 'required': False},
    'issn': {'type': 'string', 'required': False},
    'eissn': {'type': 'string', 'required': False},
    'isbn': {'type': 'string', 'required': False},
    'type': {'type': 'string', 'required': False},
    'language': {'type': 'string', 'required': False},
    'subjects': {'type': 'string', 'required': False},
    'id': {'type': 'string', 'required': True},
    'url': {'type': 'string', 'required': False}
}


def addbook(request):
    # get the request data
    request_json = request.get_json()

    validator = Validator(schema)
    valid = validator.validate(request_json)

    if not valid:
        return ("Invalid request", 400)

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

    client = bigquery.Client()
    table_id = "bookit-297317.dataset.books"
    table = client.get_table(table_id)

    query = "INSERT INTO `bookit-297317.dataset.books` (Title, Publisher, Location, ISSN, eISSN, ISBN, Type, Language, Subjects, ID, URL) VALUES  ('\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\"{}\"', '\{}\"', '\"{}\"')".format(title, publisher, location, issn, eissn, isbn, type, language, subjects, id, url)

    query_job = client.query(query)
    results = query_job.result()

    return {'success' : True}, 200
