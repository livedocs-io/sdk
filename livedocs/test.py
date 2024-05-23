from bq import evaluate_jinja_expression, split_and_replace_query, run_bigquery_query,create_bigquery_client

# Example usage:
query_template = """
SELECT
  coalesce(team, 'null') AS team,
  SUM(win_percentage) AS win_percentage
FROM
  livedocs-dev.s_ce6a3eabb5af454185458908baa33c40_ingestors.ipl_team_wise_historic
GROUP BY
  team
ORDER BY
  team ASC
"""
context = {'value': 'some_value_to_query'}
parsed_query = evaluate_jinja_expression(query_template, context)
print(parsed_query)
new = split_and_replace_query(parsed_query, "da")
print(new)


# client = create_bigquery_client(bq_cred)
# results = run_bigquery_query(parsed_query, client)

# print(results)
# for row in results:
#     print(row)