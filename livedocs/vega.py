import altair as alt
import pandas as pd


data = pd.DataFrame({"a": list("CCCDDDEEE"), "b": [2, 7, 4, 1, 2, 6, 8, 4, 7]})

chart = alt.Chart(data).mark_point().encode(x="a", y="b")

print(chart.to_json())
