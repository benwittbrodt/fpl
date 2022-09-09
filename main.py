import requests
import pandas as pd


# custom packages/functions
from func import get_data, db_engine
from db_build import *


db = db_engine()
data = get_data('main')
chips = chip_plays(data)

chips.to_sql(db, 'chipplays', index=False, if_exists='replace')
