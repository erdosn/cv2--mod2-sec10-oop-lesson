import sqlite3

import pandas as pd


class DataConnector():
    
    
    def __init__(self, filename):
        # it is common practice
        # to assign attributes in the init method
        self.conn = sqlite3.Connection(filename)


    # have a method that can list all of the table names of my db 
    def get_table_names(self):
        """
        takes in
        a sqlite3 connection
        
        returns
        the table_names from the db
        """
        query = "select name from sqlite_master where type='table'"

        res = self.conn.execute(query).fetchall()
        table_names = [r[0] for r in res]
        return table_names 
    
    
    
    def load_table_as_df(self, table_name="payments"):
        query = f"select * from {table_name}"

        df = pd.read_sql(query, self.conn)
        return df 
    
    
    
class PaymentsConnector(DataConnector):
    
    
    def __init__(self, filename):
        super().__init__(filename) # super() is new to python3
        self.table_name = "payments"
        pass
    
    
    def load_df(self):
        df = self.load_table_as_df(table_name=self.table_name)
        return df 
    
    
    def find_customers(self, customer_number=103):
        query = f"select * from payments where customerNumber = '{customer_number}';"
        df = pd.read_sql(query, self.conn)
        return df