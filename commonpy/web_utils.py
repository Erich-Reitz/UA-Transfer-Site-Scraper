import requests


def get_response(url, payload, method="GET") -> requests.Response:
    response = requests.request(method, url, data=payload)
    return response
