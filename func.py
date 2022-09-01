import requests
import pandas as pd
import json
from sqlalchemy import create_engine


def db_engine():
    db_server = 'localhost'
    db_name = 'fpl'
    db_port = '3306'
    db_username = 'root'
    db_password = 'fiorfan89'
    # Use sqlalchemy to create engine
    engine = create_engine(
        f'mysql+mysqlconnector://root:{db_password}@{db_server}/{db_name}')
    return engine


def get_data(endpoint, params={}):
    """
    Creates a url string with parameters passed in
    """
    # All possible endpoints
    endpoints = {
        'main': 'https://fantasy.premierleague.com/api/bootstrap-static/',
        'fixtures': 'https://fantasy.premierleague.com/api/fixtures/',
        'player_summary': 'https://fantasy.premierleague.com/api/element-summary/{element_id}/',
        'gameweek': 'https://fantasy.premierleague.com/api/event/{gameweek}/live/'
    }

    url = endpoints[endpoint]
    # Parse endpoint variables into urls
    if len(params) > 0:
        for key, val in params.items():
            if key in url:
                url = url.replace('{'+key+'}', str(val))

    response = requests.get(url)
    print(f'Querying the API URL: {url}')
    if response.status_code != 200:
        raise Exception("Response was code " + str(response.status_code))

    data = response.json()

    return data
