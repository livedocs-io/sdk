import psycopg2
from jinja2 import Environment, BaseLoader
import polars as pl
import re


class PostgresExecutor:
    def __init__(self):
        self.env = Environment(loader=BaseLoader())

    def parse_jinja_expression(self, expression):
        template = self.env.from_string(expression)
        undefined_string = template.undefined if hasattr(template, "undefined") else ""
        variables = undefined_string.split() if undefined_string else []
        return template, variables

    def execute_postgres_query(self, query, connection):
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        description = cursor.description
        cursor.close()
        return results, description

    def run_postgres_query(self, query, connection):
        template, variables = self.parse_jinja_expression(query)
        rendered_query = template.render()
        results, description = self.execute_postgres_query(rendered_query, connection)
        columns = [desc[0] for desc in description]
        results_df = pl.DataFrame(results, schema=columns)
        return rendered_query, results_df

    def connect_to_postgres(self, host, port, database, user, password):
        return psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )

    def choose_db_creds(self, query, creds):
        from_clause_pattern = r"\bFROM\b\s+([`a-zA-Z0-9_\-\.]+)"
        match = re.search(from_clause_pattern, query, re.IGNORECASE)
        if match:
            current_source = match.group(1)
            db_name = current_source.split(".")[0]
            return creds.get(db_name, {})
        return {}

    def close_postgres_connection(self, connection):
        connection.close()

    def parse_pg_query(self, query, db_name, pg_creds):
        current_query_creds = pg_creds[db_name]
        connection = self.connect_to_postgres(
            current_query_creds["connection_details"]["host"],
            current_query_creds["connection_details"]["port"],
            current_query_creds["connection_details"]["database"],
            current_query_creds["connection_details"]["user_name"],
            current_query_creds["connection_details"]["password"],
        )

        try:
            rendered_query, results_df = self.run_postgres_query(query, connection)
            return results_df
        finally:
            self.close_postgres_connection(connection)

    def create_pg_cred_dict(self, creds_arr):
        creds_dict = {}
        for item in creds_arr:
            creds_dict[item["connection_details"]["database"]] = item
        return creds_dict
