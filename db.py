
import pandas as pd
import psycopg2
from psycopg2 import sql as psql


DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "fiorfan89"
DB_HOST = "localhost"
DB_PORT = "5432"


def connect():
    try:
        conn = psycopg2.connect(
            database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        print("Database connected successfully")
    except:
        print("Database not connected successfully")
    return conn


def fetchall_to_df(cursor):
    columns = [x[0] for x in cursor.description]
    records = cursor.fetchall()
    df = pd.DataFrame(records, columns=columns)
    return df


def sql(conn, sql, params={}):

    with conn.cursor() as curs:
        try:
            curs.execute(sql, params)
            if curs.description:
                return fetchall_to_df(curs)
        except Exception as e:
            print('SQL Error: %s' % str(e))


param = {'db': 2}
sql1 = "SELECT * from fpl.id_only where id = %(db)s"
conn = connect()
print(
    sql(conn, sql1, param)
)

exit()


class dbTools(object):
    def __init__(self, verbose_mode=False):
        self.verbose_mode = verbose_mode

    def __del__(self):
        if getattr(self, 'conn', None) is not None:
            self.conn.close()

    def connect(self):

        user = 'postgres'
        password = 'fiorfan89'
        host = 'localhost'
        port = '5432'
        dbname = 'postgres'
        self.conn = psycopg2.connect(dbname=dbname, user=user,
                                     password=password, host=host, port=port)

        self.connected_user_name = user

    def toggle_verbose_mode(self):
        if self.verbose_mode:
            self.verbose_mode = False
        else:
            self.verbose_mode = True
        print('Verbose mode: %s' % str(self.verbose_mode))

    def compose_sql(self, query, raw_identifiers):
        if not raw_identifiers:
            query = psql.SQL(query)
        else:
            identifiers = {}
            for k, v in raw_identifiers.items():
                if type(v) == str:
                    identifiers[k] = psql.Identifier(v)
                elif type(v) == list:
                    identifiers[k] = psql.SQL(',').join(
                        [psql.Identifier(u) for u in v])
            query = psql.SQL(query).format(**identifiers)
        return query

    def sql(self, sql, params={}, identifiers=None):
        # if self.verbose_mode:
        #     print("TRYING SQL: \n" + self.preview_query(sql, params, identifiers))

        sql = self.compose_sql(sql, identifiers)

        with self.conn as conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(sql, params)
                    if curs.description:
                        return self._fetchall_to_df(curs)
                except Exception as e:
                    print('SQL Error: %s' % str(e))

    def preview_query(self, sql, params={}, identifiers=None):
        sql = self.compose_sql(sql, identifiers)

        with self.conn as conn:
            with conn.cursor() as curs:
                try:
                    return curs.mogrify(sql, params).decode('utf-8')
                except Exception as e:
                    return Exception

    def _fetchall_to_df(self, cursor):
        columns = [x[0] for x in cursor.description]
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=columns)
        return df

    def row_count(self, table):
        schema, tablename = table.split('.')
        sql = "SELECT COUNT(*) FROM {schema}.{tablename}"
        identifiers = {
            'schema': schema,
            'tablename': tablename
        }
        result = self.sql(sql=sql, identifiers=identifiers, params=None)
        return result['count'][0]

    def get_column_names(self, table, sample_size=100, include_dtypes=False):
        schema, tablename = table.split('.')
        # fetch n records (default: 100) to enable pandas to guess at datatypes
        sql = "SELECT * FROM {schema}.{tablename} LIMIT %(sample_size)s"
        identifiers = {
            'schema': schema,
            'tablename': tablename
        }
        params = {
            'sample_size': sample_size
        }
        result = self.sql(sql=sql, identifiers=identifiers, params=params)
        if include_dtypes:
            data = list(zip(result.columns, result.dtypes))
            return data
        else:
            return result.columns
