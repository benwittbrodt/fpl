import requests
import pandas as pd
import json
from sqlalchemy import create_engine


def db_engine():
    db_server = '10.0.0.41'
    db_name = 'fpl'
    db_port = '5432'
    db_username = 'postgres'
    db_password = 'fiorfan89'
    # Use sqlalchemy to create engine
    engine = create_engine(
        f'postgresql+psycopg2://postgres:{db_password}@{db_server}/{db_name}')
    return engine


def build_url(endpoint, params={}):
    """
    Creates a url string with parameters passed in
    """
    # All possible endpoints
    endpoints = {
        'main': 'https://fantasy.premierleague.com/api/bootstrap-static/',
        'fixtures': 'https://fantasy.premierleague.com/api/fixtures/',
        'player_fixtures': 'https://fantasy.premierleague.com/api/element-summary/{element_id}/{test_id}/',
        'gameweek': 'https://fantasy.premierleague.com/api/event/{gameweek}/live/'
    }

    url = endpoints[endpoint]
    # Parse endpoint variables into urls
    if len(params) > 0:
        for key, val in params.items():
            if key in url:
                url = url.replace('{'+key+'}', str(val))

    return url


def get_data(url):
    """ 
    Request data from supplied endpoint
    Returns a json data object, or a exception code
    """
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Response was code " + str(response.status_code))

    data = response.json()

    return data
