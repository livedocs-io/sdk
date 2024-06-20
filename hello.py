from livedocs import main



lib = main.Lib("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMDA2NDkzNTc1MjQ4MzkyMDE1MDUiLCJ3b3Jrc3BhY2VfaWQiOiIzY2RiYTAxNi1jZDU3LTQ1YmMtYTUzNy02NzIwYmYxMzhkNzYiLCJyZXBvcnRfaWQiOiI2ODlmNjc2NC00ZjU5LTQ1Y2EtODA2Yi1jN2E1OWRhODYzYjAiLCJ1c2VyX2ltZyI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0tNQ3JlaVlEbnpGZmp6WVJrb3dpZE5rV0hYT2FZak44UXZKZUxidzVFYzFMaGk2SnM9czk2LWMiLCJ1c2VyX25hbWUiOiJSYWFodWwgUHJlbSIsImlhdCI6MTcxODkwMjA3NiwiZXhwIjoxNzE4OTMwODc2fQ._eFc0NLqfOuTpC3lk0UykwFTbH3pEHHyEYD79-XgBhI", "dev")


# print(lib.secrets_arr)
# print(lib.pg_creds)
# Example query and usage
bq_query = '''SELECT * FROM ingestors.epl LIMIT 1000'''
context = {}  # Define the context if needed
bq_result = lib.run_bigquery(bq_query, context)
print(bq_result)

pg_query = "SELECT * FROM users"
db_name = "raahulprem"
pg_result = lib.run_postgres(pg_query, db_name)
print(pg_result)

print("\n")
print("----------------------------------")
print("\n")

# Assuming you have a DataFrame `df` and a query to run on it
df_query = "SELECT * FROM df"
df = bq_result  # Your Polars DataFrame
df_result = lib.run_dataframe(df_query,"df", df)
print(df_result)
