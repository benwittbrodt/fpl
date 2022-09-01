import pandas as pd

from func import db_engine, get_data

db = db_engine()


# def build_main(db, data=get_data('main')):
#     for key, val in data.items():
#         print(key)


def top_element(data):
    """
    creates a dataframe that has the top scoring element (player) for all gameweeks (event)
    input is the data dictionary for events from the API
    """
    highest_score = pd.DataFrame()

    i = 1
    for index in data['events']:

        item_df = pd.DataFrame(index['top_element_info'], index=[i])
        item_df['event_id'] = i
        highest_score = pd.concat([highest_score, item_df])
        i += 1

    highest_score = highest_score.reset_index().dropna()
    highest_score['id'] = highest_score['id'].astype(int)
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


def to_db(df, db, table_name, index=False, if_exists='fail'):
    """ Entering into database"""
    return df.to_sql(table_name, db, if_exists=if_exists, index=index)


def fixture_stats(data):
    """
    Creates a dataframe with just stats for each fixture 
    Input is a dictionary from the fixtures API call
    """
    i = 1
    stat_dict = {}
    for index, fixture in enumerate(data):
        fix_id = fixture['id']
        gameweek = fixture['event']
        for stat in data[index]['stats']:
            for away in stat['a']:
                print(gameweek, fix_id, stat['identifier'],
                      '| a |', away['value'], '|', away['element'])
                stat_dict[i] = [gameweek, fix_id, stat['identifier'],
                                'a', away['value'], away['element']]
                i += 1
            for home in stat['h']:
                print(gameweek, fix_id, stat['identifier'],
                      '| h |', home['value'], '|', home['element'])
                stat_dict[i] = [gameweek, fix_id, stat['identifier'],
                                'a', home['value'], home['element']]
                i += 1
    stats = pd.DataFrame.from_dict(stat_dict, columns=[
                                   'gameweek', 'fixture', 'stat_name', 'team', 'value', 'element'], orient='index')
    return stats
