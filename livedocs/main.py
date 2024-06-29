from livedocs.manager.duckdb import DuckDBSingleton
from livedocs.types import Credentials, ElementDataSource, ElementDatasourceType
import pandas as pd
import requests

"""
This is initialized in the prelude cell of the notebook like this:
    
    livedocs = Livedocs(report_id, session_token)

"""


class Livedocs:
    def __init__(self, report_id: str, token: str):
        self._duckdb = DuckDBSingleton()
        self._credentials = self._fetch_credentials(report_id, token)

    def query(self, query: str, datasource: ElementDataSource) -> pd.DataFrame:
        match datasource["sourceType"]:
            case ElementDatasourceType.database:
                return self._query_database(query, datasource)
            case ElementDatasourceType.file:
                return "file result"
            case ElementDatasourceType.dataframe:
                return "df result"
            case ElementDatasourceType.database_table:
                return "db table result"
            case _:
                return "unknown result"

    def _fetch_credentials(self, report_id: str, token: str) -> Credentials:
        print(report_id)
        response = requests.get(
            f"http://localhost:4000/v1/credentials/{report_id}",
            headers={"authorization": token},
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to fetch credentials. Status code: {response.status_code}"
            )

    def _query_database(
        self, query: str, datasource: ElementDataSource
    ) -> pd.DataFrame:
        print(self._credentials)
        connection_string = "callGetConnectionStringFromCredentials"
        alias = "alias"

        # Attach if not already attached
        self._duckdb.attach_postgres(connection_string, alias)

        # Execute the query
        result = self._duckdb.conn.execute(
            f"SELECT * FROM {alias}.({query})"
        ).fetch_df()

        return result

    # def __init__(self, auth_token, env):
    #     connection_details = get_workspace_connection_details(
    #         auth_token=auth_token, env=env
    #     )
    #     setup_secrets(connection_details["workspace_secrets"])
    #     self.env = env
    #     self.dataFrameExecutor = DataFrameQueryExecutor()
    #     self.chart_generator = ChartGenerator()
    #     self.postgresParser = PostgresExecutor()
    #     self.bigqueryParser = BigQueryExecutor(connection_details["bigquery_creds"])
    #     self.pg_creds = self.postgresParser.create_pg_cred_dict(
    #         connection_details["databases"]
    #     )
    #     self.secrets_arr = connection_details["workspace_secrets"]
    #     self.wsid = connection_details["workspace_id"]
    #     self.curr_run = datetime.now()

    # def run_bigquery(self, query, context):
    #     return self.bigqueryParser.parse_bq_query(query, context, self.wsid)

    # def run_chart(self, config, data):
    #     chart_config = self.chart_generator.generate_highcharts_config(
    #         config=config, data=data
    #     )
    #     return jsonify(chart_config)

    # def run_postgres(self, query, db_name):
    #     # load_dotenv()
    #     return self.postgresParser.parse_pg_query(query, db_name, self.pg_creds)

    # def run_dataframe(self, query, df_name, df):
    #     return self.dataFrameExecutor.run_dataframe_query(query, df_name, df)

    # def save_to_bq(self, data, dataset_id, table_id, client):
    #     return save_dataframe(data, dataset_id, table_id, client)
