# Standard library imports

# Third party imports
import mysql.connector
import pandas as pd

# Local imports
from modules.aws.secrets import fetch_secret


class MySQL:
    """
    Use me to interact with MySQL databases. The following methods are made available:
        - `query_mysql`: Executes a `select from where` SQL query and returns a tuple result set of column headers and records of data. Sequentially unpack return value, if not returned as DataFrame.
    """
    def __init__(self, default_cluster=True, cluster=None):

        if default_cluster:
            self.db_details = fetch_secret("<Insert Secret Name as stored in AWS Secrets Manager Here>")
            
        elif cluster:
            self.db_details = fetch_secret("<Insert Secret Name as stored in AWS Secrets Manager Here>")

        else:
            raise SystemExit("Error: Specified cluster does not map to a secret. Check cluster value and AWS Secrets Manager. Exiting.")

    
    def query_mysql(self, sql_query, return_df=False):
        """
        Executes a `select` statement against a MySQL db and returns the resulting column headers and data records as a tuple, or a DataFrame if `return_df` is set to True.

        Parameters
        ----------
            sql_query (str): A SQL query to fetch data from Redshift.

        Returns
        -------
            tuple: A tuple result set containing column names and data rows.
        """

        self._connect()

        self._execute(sql_query)

        self.columns = [desc[0] for desc in self.cursor.description]

        self.data = self.cursor.fetchall()

        self._disconnect()

        if return_df:
            return pd.DataFrame(data=self.data, columns=self.columns)
    
        else:
            return self.columns, self.data


    def _connect(self):
        """Establish a connection to MySQL db instance."""
        self.conn = mysql.connector.connect(
            host = self.db_details["<replace with host string key name>"],
            port = self.db_details["<replace with port key name>"],
            dbname = self.db_details["<replace with database name key name>"],
            user = self.db_details["<replace with username key name>"],
            password = self.db_details["<replace with password key name>"]
        )

        self.cursor = self.conn.cursor()

    
    def _execute(self, query, args=None):
        self.cursor.execute(query, args)


    def _disconnect(self):
        """Close the connection to MySQL db instance."""
        self.cursor.close()