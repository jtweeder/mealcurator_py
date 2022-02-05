import os
import psycopg2
import pandas as pd


class postgre():

    def __init__(self):
        self.connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                           password=os.getenv('POSTGRES_PW'),
                                           host=os.getenv('POSTGRESIP'),
                                           port=os.getenv('POSTGRESPORT'),
                                           database=os.getenv('POSTGRES_DB'))

    def select(self, sql, idx=None):
        """
        Returns a SQL Query Result as a Dataframe

        Parameters
        ----------
        sql : string
            SQL query to execute
        idx : str
            Column name to use as an index

        Returns
        -------
        select_df : DataFrame
            Results from SQL query as DataFrame
        """

        cursor = self.connection.cursor()
        cursor.execute(sql)
        select_df = pd.DataFrame.from_records(cursor.fetchall(),
                                              index=idx,
                                              columns=[col[0]
                                                       for col
                                                       in cursor.description])
        cursor.close()
        return select_df

    def insert_update(self, sql, vals):
        cursor = self.connection.cursor()
        cursor.execute(sql, vals)
        cursor.close()
        self.connection.commit()
        return

    def delete(self):
        # TODO
        return
    
