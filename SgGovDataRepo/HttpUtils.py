import urllib3
import json
from property_config import property_config

https = urllib3.PoolManager()


def build_url():
    return geturl(property_config['api'], getparams())


def getparams():
    return {'resource_id': property_config['resource_id'],
            'limit': property_config['limit'],
            'offset': property_config['offset']}


def geturl(prefix, params):
    url = ''
    for key in params:
        if len(url) == 0:
            url = url + '?'
        else:
            url = url + '&'
        url = url + key + '=' + str(params[key])
    url = prefix + url
    return url


def print_all_data():
    next_api_url = build_url()
    while len(next_api_url) > 0:
        dic = retrieve_data(property_config['host'] + next_api_url, 'GET')
        print(dic['result'])
        next_api_url = dic['result']['_links']['next']


def retrieve_data(url, method):
    response = https.request(method, url)
    dic = json.loads(response.data)
    return dic
