import pandas as pd 
import numpy as np 
import streamlit as st
import requests 

from func import get_data

# print(get_data('main'))
result = get_data('main')

events = result['events']

#print(type(result))
df = pd.DataFrame(events)
df['top_element_points'] = pd.json_normalize(df['top_element_info'])['points']

output= df[['id', 'name', 'deadline_time', 'average_entry_score', 'finished',
       'data_checked', 'most_selected',
       'most_transferred_in', 'top_element', 'top_element_points',
       'transfers_made', 'most_captained', 'most_vice_captained']]

elements = pd.DataFrame(result['elements'])
element_cols = ['first_name','second_name', 'web_name', 'element_type', 'event_points','form', 'id',
       'now_cost', 'photo', 'points_per_game',
        'selected_by_percent', 'special', 'squad_number',
       'status', 'team', 'team_code', 'total_points', 'transfers_in',
       'value_form', 'value_season', 'minutes', 'goals_scored',
       'assists', 'clean_sheets', 'goals_conceded', 'own_goals',
       'penalties_saved', 'penalties_missed', 'yellow_cards', 'red_cards',
       'saves', 'bonus', 'bps', 'influence', 'creativity', 'threat',
       'ict_index', 'starts', 'expected_goals', 'expected_assists',
       'expected_goal_involvements', 'expected_goals_conceded',
       'influence_rank', 'influence_rank_type', 'creativity_rank',
       'creativity_rank_type', 'threat_rank', 'threat_rank_type',
       'ict_index_rank', 'ict_index_rank_type',
       'corners_and_indirect_freekicks_order',
       'corners_and_indirect_freekicks_text', 'direct_freekicks_order',
       'direct_freekicks_text', 'penalties_order', 'penalties_text',
       'expected_goals_per_90', 'saves_per_90', 'expected_assists_per_90',
       'expected_goal_involvements_per_90', 'expected_goals_conceded_per_90',
       'goals_conceded_per_90', 'now_cost_rank', 'now_cost_rank_type',
       'form_rank', 'form_rank_type', 'points_per_game_rank',
       'points_per_game_rank_type', 'selected_rank', 'selected_rank_type',
       'starts_per_90', 'clean_sheets_per_90']
print(elements[element_cols])
exit()
#OUTPUTS
st.dataframe(output)
st.line_chart(output['transfers_made'])