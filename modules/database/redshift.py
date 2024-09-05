# Standard library imports

# Third party imports
import psycopg2
import pandas as pd

# Local imports
from modules.aws.secrets import fetch_secret


class Redshift:
    """
    Use me to interact with an AWS Redshift database.
    \n\nThe following methods are made available:
        - `query_redshift`: Executes a `select` statement and returns a tuple result set of column headers and records of data. Sequentially unpack return value if return_df is not set to True.
        - `load_dataframe_to_table`: Drops and rebuilds a specified table with data from a given DataFrame
    """
    
    def __init__(self):
        self.db_details = fetch_secret("<insert Secret Name as stored in AWS Secrets Manager Here>")
    

    def query_redshift(
            self,
            sql_query:str,
            return_df:bool=False
        ):
        """
        Executes a `select` statement against a Redshift database and
        returns the resulting column headers and data records as a tuple.

        Parameters
        ----------
            sql_query (str): A SQL query to fetch data from Redshift.
            return_df (bool): Boolean value that returns a DataFrame when set to True.

        Returns
        -------
            tuple: A tuple result set containing column names and data rows, or DataFrame if specified.
        """

        self._connect()

        self._execute(sql_query)
        
        # Get column names from cursor description and store as a list
        self.columns = [desc[0] for desc in self.cursor.description]

        # Store data returned by query as a list of tuples
        self.data = self.cursor.fetchall()

        self._disconnect()

        if return_df:
            return pd.DataFrame(data=self.data, columns=self.columns)
    
        else:
            return self.columns, self.data
               
        
    def load_dataframe_to_table(
            self,
            df: pd.DataFrame, 
            destination_table: str
        ):
        """
        This function drops and rebuilds a specified table with data from a given DataFrame.

        Parameters
        ----------
            df (pd.DataFrame): A DataFrame to load data to Redshift.
            destination_table (str): The name of the table to load the DataFrame to.
        """
        
        try:
            self._connect()

            # Check if destination_table exists, if it does, drop it so we can rebuild it
            self._execute(f"SELECT EXISTS (SELECT table_schema||'.'||table_name as schema_dot_table FROM information_schema.tables WHERE schema_dot_table = '{destination_table}');")
            table_exists = self.cursor.fetchone()[0]
            if table_exists:
                self._execute(f"DROP TABLE IF EXISTS {destination_table};")
                print(f"Table '{destination_table}' dropped successfully.")

            # Create destination table - potentially revisit this to leverage SHOW TABLE statement to generated CREATE TABLE statement if we know the dataframe structure will not change over time
            self._execute_create_table_query(df, destination_table)
            print(f"Table '{destination_table}' created successfully.")

            # Load DataFrame to table and commit transaction
            self._execute_insert_into_values_query(df, destination_table)
            self._commit()
            print(f"DataFrame loaded to table '{destination_table}' successfully.")
        
        except Exception as e:
            self._rollback()
            print(f"Error occurred: {e}")
        
        finally:
            self._disconnect()

    
    def _execute_create_table_query(self, df, table_name):
        from decimal import Decimal

        column_definitions = []

        for column in df.columns:
            
            non_null_series = df[column].dropna()

            if non_null_series.empty:
                column_type = 'VARCHAR(255)'

            elif pd.api.types.is_bool_dtype(non_null_series) or non_null_series.isin([True, False]).all():
                column_type = 'BOOLEAN'

            elif pd.api.types.is_integer_dtype(non_null_series) or non_null_series.apply(lambda x: isinstance(x, int)).all():
                column_type = 'INTEGER'

            elif pd.api.types.is_float_dtype(non_null_series) or non_null_series.apply(lambda x: isinstance(x, float) or isinstance(x, Decimal)).all():
                column_type = 'FLOAT'

            elif pd.api.types.is_string_dtype(non_null_series):
                max_len = non_null_series.map(lambda x: len(str(x)) if x else 0).max()
                column_type = f'VARCHAR({max_len})'

            else:
                column_type = 'VARCHAR(255)'
                
            column_definitions.append(f"{column} {column_type}")
        
        create_table_query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)});"
        print(create_table_query)
        
        self._execute(create_table_query)
    

    def _execute_insert_into_values_query(self, df, destination_table):
        from decimal import Decimal

        # Get the list of columns
        columns = ", ".join(df.columns)
        
        # Initialize an empty list to hold the formatted row strings
        value_list = []

        # Iterate over each row in the DataFrame
        for row in df.itertuples(index=False, name=None):
            
            # Initialize an empty list to hold the string representations of each value in the row
            row_values = []
            
            # Iterate over each value in the row
            for value in row:

                # Convert None values to NULL for SQL
                if value is None:
                    value_repr = 'NULL'
                
                elif isinstance(value, bool):
                    value_repr = 'TRUE' if value else 'FALSE'

                # Convert Decimal() df dtypes to float for SQL
                elif isinstance(value, Decimal):
                    value_repr = repr(float(value))
                
                # Properly format string values and escape single quotes
                elif isinstance(value, str):
                    value_repr = "'" + value.replace("'", "''") + "'"

                else:
                    # Convert the value to its string representation
                    value_repr = repr(value)

                # Append the string representation to the row_values list
                row_values.append(value_repr)
            
            # Once all row values have been processed, stage the row values as a single record to be appended to value_list for bulk insert SQL - e.g. VALUES (1, 'a', NULL), (2, 'b', 'value'), ...
            row_values_str = ", ".join(row_values)
            value_tuple_str = f"({row_values_str})"
            value_list.append(value_tuple_str)

        # Concatenate the staged rows into a single string
        values = ", ".join(value_list)
        print(values)

        # Construct the full insert query
        insert_into_values_query = f"INSERT INTO {destination_table} ({columns}) VALUES {values};"
        print(insert_into_values_query)
        
        self._execute(insert_into_values_query)


    def _get_data_type(self, dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        else:
            return 'VARCHAR(255)'


    def _connect(self):
        """Establish a connection to Redshift db instance."""
        self.conn = psycopg2.connect(
            host = self.db_details["<replace with host string key name>"],
            port = self.db_details["<replace with port key name>"],
            dbname = self.db_details["<replace with database name key name>"],
            user = self.db_details["<replace with username key name>"],
            password = self.db_details["<replace with password key name>"]
        )

        self.cursor = self.conn.cursor()

    
    def _commit(self):
        self.conn.commit()


    def _rollback(self):
        self.conn.rollback()


    def _execute(self, query, args=None):
        self.cursor.execute(query, args)


    def _disconnect(self):
        """Close the connection to our Redshift db instance."""
        self.cursor.close()
        self.conn.close()