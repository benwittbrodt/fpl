import pandas as pd
import datetime

from func import db_engine, get_data

db = db_engine()


# def build_main(db, data=get_data('main')):
#     for key, val in data.items():
#         print(key)
def season_name():
    """
    Returns the name of a FPL season eg '22_23'
    """
    now = datetime.datetime.now()
    this_yr = now.strftime('%y')
    next_yr = int(now.strftime('%y'))+1
    return f"{this_yr}_{next_yr}"


def top_element(data):
    """
    creates a dataframe that has the top scoring element (player) for all gameweeks (event)
    input is the data dictionary for events from the API
    """
    highest_score = pd.DataFrame()

    for count, event in enumerate(data['events'], start=1):

        item_df = pd.DataFrame(event['top_element_info'], index=[count])
        item_df['gw_num'] = event['id']
        highest_score = pd.concat([highest_score, item_df])

    highest_score = highest_score.dropna()
    highest_score[['id', 'points']] = highest_score[[
        'id', 'points']].astype(int)
    highest_score['season_name'] = season_name()
    highest_score.rename(columns={'id': 'element_id'}, inplace=True)
    return highest_score


def chip_plays(data):
    """
    creates a database-ready df with the number of chip plays each week
    input: data dictionary from the events API call
    """
    chip = pd.DataFrame()

    i = 1
    for index in data['events']:

        item_df = pd.DataFrame(index['chip_plays'])
        item_df['event_id'] = i
        chip = pd.concat([chip, item_df])
        i += 1

    chip['num_played'] = chip['num_played'].astype(int)
    chip = chip.reset_index()
    return chip


def to_db(df, db, table_name, **kwargs):
    """ Entering into database"""
    return df.to_sql(table_name, db, kwargs)


def fixture_stats(data):
    """
    Creates a dataframe with just stats for each fixture 
    Input is a dictionary from the fixtures API call
    Output: dataframe will the fixture stats for every game played so far
    """
    i = 1
    stat_dict = {}
    for index, fixture in enumerate(data):
        fix_id = fixture['id']
        home_team = fixture['team_h']
        away_team = fixture['team_a']
        gameweek = fixture['event']
        fixture_id = fixture['code']
        for stat in data[index]['stats']:
            for away in stat['a']:

                stat_dict[i] = [fixture_id, gameweek, fix_id, stat['identifier'],
                                away_team, away['value'], away['element']]
                i += 1
            for home in stat['h']:

                stat_dict[i] = [fixture_id, gameweek, fix_id, stat['identifier'],
                                home_team, home['value'], home['element']]
                i += 1
    stats = pd.DataFrame.from_dict(stat_dict, columns=[
                                   'fixture_id', 'gameweek', 'fixture_num', 'stat_name', 'team_id', 'value', 'element_id'], orient='index')
    stats['season_name'] = season_name()
    return stats


def fixtures(data):
    """
    Input: data dictionary from API call of endpoint 'fixtures'
    Output: dataframe of all fixtures for the season 
    """
    to_rename = {'code': 'id',
                 'id': 'season_fixture',
                 'event': 'gameweek',
                 'team_a': 'team_a_id',
                 'team_h': 'team_h_id'}
    df = pd.DataFrame(data)
    df.drop(['stats'], axis=1, inplace=True)
    df['season_name'] = season_name()
    df.rename(columns=to_rename, inplace=True)
    return df
