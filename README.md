примеры запросов:

url = http://127.0.0.1:5000/

POST  url/advertisements/

json
{    "description": "a",
    "headline": "q",
    "Owner": "Родион"}

GET url/advertisements/1

PATCH url/advertisements/1

json
{"Owner": "Алиса"}

DELETE url/advertisements/1
