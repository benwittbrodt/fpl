import requests
import pandas as pd
import json


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

# Connect to SQLite DB and execute SQL


def fetchall_to_df(cursor):
    columns = [x[0] for x in cursor.description]
    records = cursor.fetchall()
    df = pd.DataFrame(records, columns=columns)
    return df


def sql(sql, curs):
    # check to see if the connection is closed due to timeout
    # if self.conn.closed != 0:
    #     self.connect(self.connected_profile_name)
    curs.execute(sql)
    if curs.description:
        return fetchall_to_df(curs)
