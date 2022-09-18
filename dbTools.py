import os
import psycopg2
from psycopg2 import sql as psql
import json
import base64
import pandas as pd


class dbTools(object):
    def __init__(self, verbose_mode=False):
        self._load_profiles()
        self.verbose_mode = verbose_mode
        self.connected_profile = None

    def __del__(self):
        if getattr(self, 'conn', None) is not None:
            self.conn.close()

    def toggle_verbose_mode(self):
        if self.verbose_mode:
            self.verbose_mode = False
        else:
            self.verbose_mode = True
        print('Verbose mode: %s' % str(self.verbose_mode))

    def _load_profiles(self, json_file=None):
        if not json_file:
            json_file = os.path.join(
                os.path.expanduser("~"), 'projects', 'fpl', 'profile.json')
        with open(json_file, 'r') as fh:
            profiles = json.load(fh)
        for p in profiles:
            p['password'] = self._encode_secret(p['password'])
        self.profiles = profiles
        return profiles

    def _encode_secret(self, text):
        return base64.b64encode(text.encode('utf-8'))

    def _decode_secret(self, secret):
        return base64.b64decode(secret).decode('utf-8')

    def connect(self, profile_name):
        if not self.profiles:
            self._load_profiles()
        profile = [p for p in self.profiles if p['profile_name']
                   == profile_name].pop()
        user = profile['user']
        password = self._decode_secret(profile['password'])
        host = profile['host']
        port = profile['port']
        dbname = profile['dbname']
        self.conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port)
        # self.connected_profile_name = profile['profile_name']
        # self.connected_user_name = profile['user']
        self.register_adapters()

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
        # check to see if the connection is closed due to timeout
        # if self.conn.closed != 0:
        #     self.connect(self.connected_profile_name)

        if self.verbose_mode:
            print("TRYING SQL: \n" + self.preview_query(sql, params, identifiers))

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

    def register_adapters(self):
        DEC2FLOAT = psycopg2.extensions.new_type(
            psycopg2.extensions.DECIMAL.values,
            'DEC2FLOAT',
            lambda value, curs: float(value) if value is not None else None)
        psycopg2.extensions.register_type(DEC2FLOAT)

    def show_running_queries(self):
        sql = """
            SELECT *
            FROM stv_recents
            WHERE status = 'Running'
            AND user_name = %(user_name)s
        """
        params = {
            'user_name': [p for p in self.profiles if p['profile_name'] == self.connected_profile_name][0]['user'],
        }
        result = self.sql(sql, params)
        return result

    def cancel_running_queries(self):
        queries = self.show_running_queries()
        if len(queries) > 0:
            sql = "CANCEL %(pid)s"
            params = {}
            for q in queries.to_dict(orient='records'):
                params['pid'] = q['pid']
                print('Cancelling query #%d...' % params['pid'])
                self.sql(sql, params)

    # def to_s3(self, object_name, file_name=None, df=None, bucket='sermo-data-science', top_level_prefix='analytics', index=False):
    #     s3_client = boto3.client('s3')
    #     boto3.set_stream_logger('boto3.resources', logging.INFO)
    #     object_name = '%s/%s' % (top_level_prefix, object_name)

    #     if df is not None and file_name is not None:
    #         raise ValueError(
    #             'Please pass either a dataframe or a filename, not both!')
    #     elif df is not None:
    #         csv_buffer = io.BytesIO()
    #         w = io.TextIOWrapper(csv_buffer)
    #         df.to_csv(w, compression='gzip', index=index)
    #         w.seek(0)
    #         s3_client.upload_fileobj(csv_buffer, bucket, object_name)
    #     elif file_name is not None:
    #         s3_client.upload_file(file_name, bucket, object_name)
    #     else:
    #         raise ValueError('Please pass either a dataframe or a filename.')

    # def from_s3(self, object_name, file_name, bucket='sermo-data-science', top_level_prefix='analytics'):
    #     s3 = boto3.client('s3')
    #     object_name = '%s/%s' % (top_level_prefix, object_name)
    #     s3.download_file(bucket, object_name, file_name)

    # def df_to_table(self, df, table, index=False, bucket='sermo-data-science'):
    #     schema, tablename = table.split('.')
    #     columns_and_types = self._compose_pg_dtypes_from_df(df, index)
    #     # create an empty table with the required DDL
    #     sql = """
    #             DROP TABLE IF EXISTS {schema}.{tablename};
    #             CREATE TABLE {schema}.{tablename} (
    #         """
    #     params = {}
    #     identifiers = {
    #         'schema': schema,
    #         'tablename': tablename
    #     }
    #     for i, c in enumerate(columns_and_types):
    #         if i > 0:
    #             sql += ', \n'
    #         column_name = 'column_name_%d' % i
    #         column_dtype = 'column_dtype_%d' % i
    #         sql += '{%s} %%(%s)s' % (column_name, column_dtype)
    #         identifiers[column_name] = c[0]
    #         params[column_dtype] = AsIs(c[1])

    #     sql += ');'
    #     self.sql(sql, params=params, identifiers=identifiers)

    #     # copy the file to S3
    #     top_level_prefix = 'temp-uploads'
    #     object_name = '%s_%s' % (schema, tablename)
    #     self.to_s3(object_name, df=df, top_level_prefix=top_level_prefix,
    #                bucket=bucket, index=index)

    #     # copy data into the table
    #     sql = """
    #             COPY {schema}.{tablename}
    #             FROM %(s3_path)s
    #             REGION 'us-east-1'
    #             CREDENTIALS
    #             %(s3_credentials)s
    #             CSV IGNOREHEADER AS 1

    #         """
    #     params['s3_path'] = 's3://%s/%s/%s' % (bucket,
    #                                            top_level_prefix, object_name)
    #     params['s3_credentials'] = 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'.format(
    #         **self._lookup_access_keys())
    #     self.sql(sql, params=params, identifiers=identifiers)

    def _compose_pg_dtypes_from_df(self, df, index=False):
        def pd_dtype_to_redshift_dtype(dtype):
            if dtype.startswith('int64'):
                return 'BIGINT'
            elif dtype.startswith('int'):
                return 'INTEGER'
            elif dtype.startswith('float'):
                return 'FLOAT8'
            elif dtype.startswith('datetime'):
                return 'TIMESTAMP'
            elif dtype == 'bool':
                return 'BOOLEAN'
            else:
                return 'VARCHAR'

        column_data_types = [pd_dtype_to_redshift_dtype(dtype.name)
                             for dtype in df.dtypes.values]
        column_names = df.columns.tolist()
        if index:
            column_data_types.insert(
                0, pd_dtype_to_redshift_dtype(df.index.dtype.name))
            column_names.insert(0, 'idx')
        columns_and_types = list(zip(column_names, column_data_types))
        return columns_and_types

    # def _lookup_access_keys(self):
    #     aws_creds_file = os.path.join(
    #         os.path.expanduser("~"), '.aws', 'credentials')
    #     try:
    #         with open(aws_creds_file, 'r') as fh:
    #             txt = fh.readlines()
    #     except:
    #         raise ValueError('AWS credentials not found!')

    #     aws_access_key_id = [t.split('=')[1] for t in txt if t.startswith(
    #         'aws_access_key_id')][0].strip()
    #     aws_secret_access_key = [t.split('=')[1] for t in txt if t.startswith(
    #         'aws_secret_access_key')][0].strip()
    #     return {'aws_access_key_id': aws_access_key_id, 'aws_secret_access_key': aws_secret_access_key}
