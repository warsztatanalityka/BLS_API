import json
import datetime
from sqlalchemy import create_engine
import urllib
import requests
import pandas as pd


class BlsData:

    def __init__(self, start_end_y, end_y, data_down_list, name_list, db_name, freq, replacement, reg_key):
        self.start_y = start_end_y
        self.end_y = end_y
        self.data_down_list = data_down_list
        self.name = name_list
        self.db_name = db_name
        self.freq = freq
        self.replacement = replacement
        self.reg_key = reg_key

    def download_new(self):
        # function which gather all essential code
        # returns data frame ready to add (as new)
        json_data = self.return_json()
        if self.is_date_valid(json_data) is None:
            return None
        bls_df = self.concat_json(json_data)
        return bls_df

    def insert_new_sql(self):
        # add new data to sql server
        downloaded_df = self.download_new()
        if downloaded_df is None:
            return None
        SqlMethods.bls_df_to_sql(self.db_name, downloaded_df)

    def update_existing(self, update_from):
        # update last 5 nulls or find new (if any exist) with new existing data
        # they can come up with other data
        # because we concat multiple tables
        self.start_y = update_from.year
        self.end_y = datetime.datetime.now().year
        downloaded_df = self.download_new()
        if downloaded_df is None:
            return None
        df_date = self.adj_data_before_insert(downloaded_df, update_from)
        SqlMethods.bls_df_to_sql(self.db_name, df_date)

    @staticmethod
    def adj_data_before_insert(df_to_adjust, date):
        # reduce amount of data to insert
        # smallest portion of data from bls is 1 year
        reduced_df_t_f = df_to_adjust['date'] >= pd.Timestamp(date)
        reduced_df = df_to_adjust.where(reduced_df_t_f).dropna(how='all')
        return reduced_df

    def return_json(self):
        dict_args = {"seriesid": self.data_down_list, "startyear": self.start_y, "endyear": self.end_y, "registrationkey": self.reg_key}
        headers = {'Content-type': 'application/json'}
        data = json.dumps(dict_args)
        p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        json_data = json.loads(p.text)
        return json_data

    @staticmethod
    def is_date_valid(json_data):
        # https://www.bls.gov/developers/api_faqs.htm#errors3
        # find errors
        err_inf = json_data['message']
        items_to_add = json_data['Results']['series']
        if len(items_to_add) == 0:
            print('The is no new data to add')
            return None
        if len(err_inf) == 0:
            print('Correct query, proceed...')
            return True
        if len(err_inf) != 0:
            for series_counter, series in enumerate(items_to_add):
                if len(json_data['Results']['series'][series_counter]['data']) == 0:
                    print('There is no data in your data frame')
                    string_to_check = json_data['Results']['series'][series_counter]['seriesID']
                    print(f'Please check {string_to_check}')
                    print('Function has been shut down')
                    return None
            for err_count, item in enumerate(range(len(err_inf))):
                print(f'{err_inf[err_count]}')
            print('But still there is few data to add, proceed...')
            return True

    def concat_json(self, json_data):
        # create list of df from json
        df_list = []
        for counter, json_d in enumerate(json_data['Results']['series']):
            df_json = pd.DataFrame.from_dict(json_d['data'])
            df_list.append(df_json)
        return self.create_date(df_list)

    def clear_nulls(self):
        # find null in any cells in last 5 row
        df = SqlMethods.query_sql(
            f'select * from (select top 5* from {self.db_name} order by date desc) as t5 order by date asc')
        find_nulls = df.iloc[:, 1:].isnull()
        merge_t_f = pd.Series.any(find_nulls, axis=1)
        display_true_only = df['date'].where(merge_t_f).dropna(how='all')
        if display_true_only.size == 0:
            print(f'There is no nulls in {self.db_name}.')
            return True
        else:
            del_data_from = display_true_only.iloc[0]
            SqlMethods.return_engine().execute(
                f'''delete from {self.db_name} where date >= '{del_data_from}' ''')
            return del_data_from

    def update_new_or_null(self):
        # pass date to update data
        up_n_null = self.clear_nulls()
        if up_n_null is True:
            max_date = SqlMethods.query_sql(f'select max(date) from {self.db_name}')
            SqlMethods.return_engine().execute(
                f'''delete from {self.db_name} where date >= '{max_date.iloc[0, 0]}' ''')
            return self.update_existing(max_date.iloc[0, 0])
        else:
            return self.update_existing(up_n_null)

    def create_date(self, df_list):
        # create, concat, rename, sort
        # prepare df to insert into sql table
        concat_df = pd.DataFrame()
        if self.freq == 'M' or self.freq == 'Q':
            for counter, df_bls in enumerate(df_list):
                df_bls['period'].replace(self.replacement, regex=True, inplace=True)
                df_bls['year'] = df_bls['year'].astype('str')
                df_bls['date'] = df_bls['year'] + df_bls['period']
                df_bls['date'] = pd.to_datetime(df_bls['date'], format='%Y/%m/%d')
                df_bls = df_bls.rename(columns={'value': self.name[counter]})
                df_bls = df_bls.set_index(['date'])
                concat_df = pd.concat([concat_df, df_bls[self.name[counter]]], axis=1)
            concat_df = concat_df.reset_index()
            concat_df = concat_df.rename(columns={'index': 'date'})
            concat_df = concat_df.sort_values(by=['date'])
            return concat_df
        if self.freq == 'Y':
            pass


class SqlMethods:
    # sql static methods for basic commands
    @staticmethod
    # https://docs.sqlalchemy.org/en/13/dialects/mssql.html
    def return_engine():
        params = urllib.parse.quote_plus(
            r'DRIVER={SQL Server Native Client 11.0};SERVER=DS;DATABASE=Finance;Trusted_Connection=yes')
        conn_str = f'mssql+pyodbc:///?odbc_connect={params}'
        engine = create_engine(conn_str)
        return engine

    @staticmethod
    def query_sql(query_sql):
        data = pd.read_sql_query(sql=query_sql, con=SqlMethods.return_engine())
        return data

    @staticmethod
    def bls_df_to_sql(db_name, bls_df):
        bls_df.to_sql(name=db_name, con=SqlMethods.return_engine(), index=False, if_exists='append')
        print(f'Your data has been added, from {bls_df.iloc[0, 0]} to {bls_df.iloc[-1, 0]} to your database: {db_name}')

# two instances, for monthly and quarterly data
# i did not find interesting annual data
# in each instances single table with data
# almost each parameter of instances must be provided
# last parameter is for registration key
# with reg_key we can update more data and pass more query
# info https://www.bls.gov/developers/api_faqs.htm#register1
# Do I have to register to use the BLS Public Data API?


quarterly = BlsData(2013, 2020, ['PRS85006092', 'PRS85006112', 'PRS85006152'],
                    ['Labor_productivity_h', 'Unit_labor_costs','Real_hourly_compensation'], 'q_bls', 'Q', {'Q01': '-03', 'Q02': '-06', 'Q03': '-09', 'Q04': '-12'}, "")

monthly = BlsData(2013, 2020, ['CUUR0000SA0', 'SUUR0000SA0', 'PCU327320327320', 'LNS14000000'],
                  ['CPI-U_USA', 'Employment_USA','PPI_Industry_Data','Unemployment_Rate'], 'm_bls', 'M', {'M': '-'}, "")


def insert_new():
    # place where can be call out single instance
    quarterly.insert_new_sql()
    monthly.insert_new_sql()


def update_ex():
    monthly.update_new_or_null()
    quarterly.update_new_or_null()



