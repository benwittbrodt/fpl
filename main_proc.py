# built in pip packages
import requests
import pandas as pd
import sqlite3
import json
from pprint import pprint

# custom packages/functions
from func import get_data
from db_build import *

## Main endpoint ##
# events - game weeks (FPL info)
# game_settings
# phases
# teams
# total_players
# elements - player data, updated regularly. Need to split into other tables to append new data
# element_stats
# element_types - player position metadata

p = get_data('main')
