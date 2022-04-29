# built in pip packages
import requests
import pandas as pd
import numpy as np
import psycopg2
from tabulate import tabulate

# custom packages/functions
from func import *

query = build_url('main')
teams = pd.DataFrame(get_data(query)['teams'])
json = get_data(query)
elements_df = pd.DataFrame(json['elements'])
elements_types_df = pd.DataFrame(json['element_types'])
teams_df = pd.DataFrame(json['teams'])

print(elements_df['id'].max())
