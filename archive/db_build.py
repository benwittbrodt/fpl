import sqlite3
import pandas as pd
import datetime

## Utilities ##

# DB connection


def db_conn():
    return sqlite3.connect('db/fpl.db')


def season_name():
    """
    Returns the name of a FPL season eg '22_23'
    """
    now = datetime.datetime.now()

    this_yr = now.strftime('%y')
    next_yr = int(now.strftime('%y'))+1
    last_yr = int(now.strftime('%y'))-1

    if now.month in [8, 9, 10, 11, 12]:
        return f"{this_yr}_{next_yr}"
    elif now.month in [1, 2, 3, 4, 5]:
        return f"{last_yr}_{this_yr}"


# Main endpoint

# Main -> Teams
def teams(data, write_to_db=False, conn=db_conn()):
    df = pd.DataFrame(data['teams'])
    df['season_name'] = season_name()
    df.rename(columns={'id': 'season_team_id',
              'code': 'id'}, inplace=True)

    if write_to_db:
        df.to_sql('team', conn, if_exists='replace', index=False)

    return df

# Main -> Elements


def elements(data, write_to_db=False, conn=db_conn()):
    df = pd.DataFrame.from_dict(data['elements'])
    df['season_name'] = season_name()
    df.rename(columns={
        'code': 'id',
        'id': 'season_player_id',
        'team': 'season_team_id',
        'team_code': 'team_id'}, inplace=True)
    for item in ['team_id', 'points_per_game', 'season_player_id', 'web_name', 'second_name', 'first_name', 'id']:
        col_to_move = df.pop(item)
        df.insert(0, item, col_to_move)

    if write_to_db:
        df.to_sql('player', conn, if_exists='replace', index=False)

    return df


def element_types(data, write_to_db=False, conn=db_conn()):
    df = pd.DataFrame.from_dict(data['element_types'])
    df['season_name'] = season_name()
    df.pop('sub_positions_locked')
    if write_to_db:
        df.to_sql('player_type', conn, if_exists='replace', index=False)
    return df


##### 2023 edits above this line #####

# Main -> Events


def events(data):
    """
    creates dataframe for the events endpoint (without top_element_info and chip_plays)
    """
    df = pd.DataFrame(data['events'])
    df.drop(['deadline_time_epoch', 'top_element_info',
            'chip_plays'], axis=1, inplace=True)
    df.fillna(0, inplace=True)
    convert_cols = ['highest_scoring_entry', 'highest_score', 'most_selected',
                    'most_transferred_in', 'top_element', 'most_captained', 'most_vice_captained']
    df[convert_cols] = df[convert_cols].astype(int)
    df['deadline_time'] = pd.to_datetime(
        df['deadline_time']).dt.tz_localize(None)
    df['season_name'] = season_name()
    id_str = str.split(season_name(), '_')
    df['id'] = id_str[0] + id_str[1] + df['id'].astype('str')
    return df


def top_element(data):
    """
    creates a dataframe that has the top scoring element (player) for all gameweeks (event)
    input is the data dictionary for events from the API
    """
    highest_score = pd.DataFrame()
    id_str = str.split(season_name(), '_')
    for count, event in enumerate(data['events'], start=1):

        item_df = pd.DataFrame(event['top_element_info'], index=[count])
        item_df['event_id'] = f'{id_str[0]}{id_str[1]}{count}'
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
    pd.to_sql needs index=False
    """

    chip = pd.DataFrame()
    id_str = str.split(season_name(), '_')

    i = 1
    for index in data['events']:

        item_df = pd.DataFrame(index['chip_plays'])
        item_df['event_id'] = f'{id_str[0]}{id_str[1]}{i}'
        chip = pd.concat([chip, item_df])
        i += 1

    chip['num_played'] = chip['num_played'].astype(int)
    chip = chip.reset_index()
    chip.drop('index', axis=1, inplace=True)
    return chip


def element_process(data, table):
    """
    Processes all of the data into specific tables from the elements endpoint
    """
    if table == 'element_stat_calc':
        filter_cols = ['bonus', 'bps', 'influence', 'creativity', 'threat', 'ict_index', 'influence_rank', 'influence_rank_type',
                       'creativity_rank', 'creativity_rank_type', 'threat_rank', 'threat_rank_type', 'ict_index_rank', 'ict_index_rank_type', 'corners_and_indirect_freekicks_order',
                       'corners_and_indirect_freekicks_text', 'direct_freekicks_order', 'direct_freekicks_text', 'penalties_order', 'penalties_text']
    elif table == 'element_stat_played':
        filter_cols = ['minutes', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded',
                       'own_goals', 'penalties_saved', 'penalties_missed', 'yellow_cards', 'red_cards', 'saves']
    elif table == 'element_news':
        filter_cols = ['news', 'news_added']
    elif table == 'element_price1':
        filter_cols = ['cost_change_event', 'cost_change_event_fall', 'cost_change_start',
                       'cost_change_start_fall', 'transfers_in', 'transfers_in_event', 'transfers_out', 'transfers_out_event']
    elif table == 'element_info':
        filter_cols = ['element_type', 'now_cost', 'first_name', 'second_name',
                       'dreamteam_count', 'form', 'value_form', 'photo', 'team_code', 'web_name']
    elif table == 'element_season_stat':
        filter_cols = ['points_per_game', 'total_points', 'value_season']
    elif table == 'element_event_info':
        filter_cols = ['chance_of_playing_next_round', 'chance_of_playing_this_round', 'expectedpt_next',
                       'expectedpt_this', 'event_points', 'in_dreamteam', 'status', 'selected_by_percent']

    df = pd.DataFrame(data['elements'])
    out_df = df[['code', ]+filter_cols].copy()
    out_df['season_name'] = season_name()
    out_df.rename(columns={'code': 'element_id'}, inplace=True)

    if table == 'element_news':
        out_df = out_df.dropna()
    return out_df


# Fixture endpoint


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

# Fixture endpoint


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
