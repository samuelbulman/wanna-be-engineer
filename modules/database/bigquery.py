# Standard library imports
import pandas as pd

# Third party imports
from google.cloud import bigquery

# Local imports
from modules.aws.secrets import fetch_secret


class BigQuery:
    """
    Use me to interact with a Google BigQuery instance. Update the intializer method to direct to the service account credentials that relate to the BigQuery instance you'd like to connect to.
    \n\nThe following methods are made available:
        - `execute_bigquery_query`: Executes a SQL query against respective BigQuery instance and, if applicable, returns the query's results as a job object, or a dataframe if `return_df` is set to True.
        - `load_dataframe_to_bigquery_table`: Loads a pandas DataFrame into a specified table in one of our specific BigQuery instances.
    """

    def __init__(self, instance):        
        self.secrets = fetch_secret("<insert Secret Name as stored in AWS Secrets Manager Here>")

        if instance == "<insert string here>":
            self.sa_json = fetch_secret("<insert name to respective GCP service account secret>")
            self.project_id = self.sa_json["project_id"]
        
        elif instance == "<insert string here>":
            self.sa_json = fetch_secret("<insert name to respective GCP service account secret>")
            self.project_id = self.sa_json["project_id"]

    
    def execute_bigquery_query(
            self,
            sql_query:str,
            return_df:bool=False
        ):
        """
        Takes a SQL query as a parameter and executes it in the respective BigQuery instance. By default, the
        query's results are returned as a job object but will be returned as a dataframe if `return_df` is set to True.
        """
        self.client = self._create_client()

        self.job_config = bigquery.QueryJobConfig(use_legacy_sql=False) # This is set to False to enable UPDATE DML statements.

        # Execute query against BigQuery instance
        self.results = self.client.query(
            sql_query,
            job_config=self.job_config
        )

        self.results_for_df = self.results.result()

        if return_df:
            # Convert results to a DataFrame
            return pd.DataFrame(data=[row.values() for row in self.results_for_df], columns=[column.name for column in self.results_for_df.schema])
        
        else:
            return self.results
    

    def load_dataframe_to_bigquery_table(
            self,
            dataframe: pd.DataFrame,
            schema_dot_table: str,
            write_disposition: str
        ):
        """Loads a pandas DataFrame into a specified table in a specific BigQuery instance."""

        self.client = self._create_client()

        if write_disposition:
            self.job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=write_disposition
            )
        
        else:
            self.job_config = bigquery.LoadJobConfig(
                autodetect=True
            )
        
        self.job = self.client.load_table_from_dataframe(
            dataframe, 
            schema_dot_table,
            job_config=self.job_config
        )

        self.job.result()


    def _create_client(self):
        """Return a client connection to a specific BigQuery instance."""

        self.client = bigquery.Client.from_service_account_info(
            info=self.sa_json,
            project=self.project_id
        )

        return self.client