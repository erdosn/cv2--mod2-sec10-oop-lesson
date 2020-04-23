import pandas as pd
import sqlite3


class MyDatabase():
    
    def __init__(self, filename=None): # dunders
        self.create_connection(filename=filename)
        self.table_name = None
    
    
    def create_connection(self, filename):
        # creating our connection to our sqlite file
        self.connection = sqlite3.connect(filename)
        # creating our cursor attribute
        self.api = self.connection.cursor()


    def get_table_list(self):
        query = "select name from sqlite_master where type='table';"
        table_names_result = self.api.execute(query).fetchall()
        table_names = [result[0] for result in table_names_result]
        return table_names


    def build_select_all_query(self, table_name=None):
        table_name = table_name if not self.table_name else self.table_name
        query = f"select * from {table_name}"
        return query


    # select everything from table
    def select_all_table(self, table_name=None):
        query = self.build_select_all_query(table_name)
        res = self.api.execute(query).fetchall()
        return res

    
    # show table in dataframe
    def load_table_as_df(self, table_name=None):
        query = self.build_select_all_query(table_name)
        df = pd.read_sql(query, self.connection)
        return df


    # get column information
    def get_table_columns(self, table_name=None):
        table_name = table_name if not self.table_name else self.table_name
        query = f"PRAGMA table_info({table_name})"
        res = self.api.execute(query).fetchall()
        return res


class Customers(MyDatabase):

    def __init__(self, filename=None):
        MyDatabase.__init__(self, filename=filename)
        self.table_name = 'customers'
        
    
    def build_select_by_country_query(self, countries):
        """
        countries: list/collection of countries to choose from
        """
        country_list = ','.join(f"'{country.lower()}'" for country in countries)
        query = f"select * from customers where lower(country) in ({country_list});"
        return query
    
    
    def select_customers_by_country(self, countries):
        """
        countries: list/collection of countries to choose from
        """
        query = self.build_select_by_country_query(countries)
        res = self.api.execute(query).fetchall()
        return res

    
    def select_customers_by_country_as_df(self, countries):
        """
        countries: list/collection of countries to choose from
        """
        query = self.build_select_by_country_query(countries)
        df = self.load_query_as_df(query)
        return df
    
    
    def load_query_as_df(self, query):
        df = pd.read_sql(query, self.connection)
        return df