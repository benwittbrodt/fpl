import pandas as pd

from func import db_engine, build_url, get_data

db = db_engine()


def build_main(db, data=get_data(build_url('main'))):
    for key, val in data.items():
        print(key)


build_main(db)
