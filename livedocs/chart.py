import polars as pl
from datetime import datetime, timedelta

class ChartGenerator:
    def __init__(self):
        self.default_height = 380
        self.base_chart_config = {
            "title": {"text": None},
            "credits": {"enabled": False},
            "chart": {"height": self.default_height},
            "legend": {
                "align": "center",
                "verticalAlign": "top",
                "layout": "horizontal",
            },
        }
        self.highcharts_configurations = {
            "line_graph": {"type": "line", },
            "spline_graph": {
                "type": "spline",
            },
            "stacked_area_chart": {
                "type": "area",
                "plotOptions": {"area": {"stacking": "normal"}},
            },
            "area_chart": {"type": "area"},
            "percentage_stacked_area": {
                "type": "area",
                "plotOptions": {"area": {"stacking": "percent"}},
            },
            "area_range_chart": {"type": "arearange"}, # ------------------------------------------
            "stream_graph": {"type": "streamgraph"},
            "scatterplot": {"type": "scatter"},
            "bar_graph": {"type": "column"},
            "horizontal_bar_graph": {"type": "bar"},
            "stacked_bar_graph": {
                "type": "column",
                "plotOptions": {"column": {"stacking": "normal"}},
            }, 
            "grouped_bar_graph": {
                "type": "column",
                "plotOptions": {"column": {"stacking": "normal"}},
            }, 
            "pie_chart": {"type": "pie"},
            "bubble_chart": {"type": "bubble"},
            "heatmap": {"type": "heatmap"}, # ------------------------------------------
        }

    def filter_dataframe(self, df, column, filter_type, filter_value):
        if filter_type in ["Last week", "Last 30 days", "Last 90 days", "Last year", "After date", "Before date", "Show all"]:
            return self.filter_date(df, column, filter_type, filter_value)
        elif filter_type in ["Starts with", "Ends with", "Equals", "Not equals", "Is null", "Is not null", "Show all"]:
            return self.filter_text(df, column, filter_type, filter_value)
        elif filter_type in ["Greater than", "Greater than equal to", "Less than", "Less than equal to", "between", "Is null", "Is not null", "Show all"]:
            return self.filter_numeric(df, column, filter_type, filter_value)
        else:
            return df

    def filter_date(self, df, column, filter_type, filter_value):
        today = datetime.today()
        
        if filter_type == 'Last week':
            last_week = today - timedelta(weeks=1)
            return df.filter(pl.col(column) >= last_week)
        elif filter_type == 'Last 30 days':
            last_30_days = today - timedelta(days=int(filter_value))
            return df.filter(pl.col(column) >= last_30_days)
        elif filter_type == 'Last 90 days':
            last_90_days = today - timedelta(days=int(filter_value))
            return df.filter(pl.col(column) >= last_90_days)
        elif filter_type == 'Last year':
            last_year = today.replace(year=today.year - 1)
            return df.filter(pl.col(column) >= last_year)
        elif filter_type == 'After date':
            date = datetime.strptime(filter_value, '%Y-%m-%d')
            return df.filter(pl.col(column) > date)
        elif filter_type == 'Before date':
            date = datetime.strptime(filter_value, '%Y-%m-%d')
            return df.filter(pl.col(column) < date)
        elif filter_type == 'Show all':
            return df
        else:
            raise ValueError(f"Unknown filter value: {filter_value}")

    def filter_text(self, df, column, filter_type, filter_value):
        if filter_type == 'Starts with':
            return df.filter(pl.col(column).str.starts_with(filter_value))
        elif filter_type == 'Ends with':
            return df.filter(pl.col(column).str.ends_with(filter_value))
        elif filter_type == 'Equals':
            return df.filter(pl.col(column) == filter_value)
        elif filter_type == 'Not equals':
            return df.filter(pl.col(column) != filter_value)
        elif filter_type == 'Is null':
            return df.filter(pl.col(column).is_null())
        elif filter_type == 'Is not null':
            return df.filter(pl.col(column).is_not_null())
        elif filter_type == 'Show all':
            return df
        else:
            raise ValueError(f"Unknown filter value: {filter_value}")

    def filter_numeric(self, df, column, filter_type, filter_value):
        if filter_type == 'Greater than':
            return df.filter(pl.col(column) > float(filter_value))
        elif filter_type == 'Greater than equal to':
            return df.filter(pl.col(column) >= float(filter_value))
        elif filter_type == 'Less than':
            return df.filter(pl.col(column) < float(filter_value))
        elif filter_type == 'Less than equal to':
            return df.filter(pl.col(column) <= float(filter_value))
        elif filter_type == 'between':
            num1, num2 = map(float, filter_value.split(','))
            return df.filter((pl.col(column) >= num1) & (pl.col(column) <= num2))
        elif filter_type == 'Is null':
            return df.filter(pl.col(column).is_null())
        elif filter_type == 'Is not null':
            return df.filter(pl.col(column).is_not_null())
        elif filter_type == 'Show all':
            return df
        else:
            raise ValueError(f"Unknown filter value: {filter_value}")
        
    def aggregate_dataframe(self, df, group_by_column, agg_dict):
        """
        Aggregates a DataFrame based on the specified group_by_column and a dictionary of aggregation types for other columns.
        
        Parameters:
        - df (polars.DataFrame): The DataFrame to aggregate.
        - group_by_column (str): The column to group by.
        - agg_dict (dict): A dictionary where keys are column names and values are the aggregation types.
        
        Returns:
        - polars.DataFrame: The aggregated DataFrame.
        """
        agg_exprs = []
        for col in agg_dict:
            name = col["column_name"]
            if col["aggregate"] == 'sum':
                agg_exprs.append(pl.col(name).sum().alias(f"{name}"))
            elif col["aggregate"] == 'average':
                agg_exprs.append(pl.col(name).mean().alias(f"{name}"))
            elif col["aggregate"] == 'count':
                agg_exprs.append(pl.col(name).count().alias(f"{name}"))
            elif col["aggregate"] == 'min':
                agg_exprs.append(pl.col(name).min().alias(f"{name}"))
            elif col["aggregate"] == 'max':
                agg_exprs.append(pl.col(name).max().alias(f"{name}"))
            elif col["aggregate"] == 'median':
                agg_exprs.append(pl.col(name).median().alias(f"{name}"))
            elif col["aggregate"] == 'std':
                agg_exprs.append(pl.col(name).std().alias(f"{name}"))
            elif col["aggregate"] == 'var':
                agg_exprs.append(pl.col(name).var().alias(f"{name}"))
            elif col["aggregate"] == 'first':
                agg_exprs.append(pl.col(name).first().alias(f"{name}"))
            elif col["aggregate"] == 'last':
                agg_exprs.append(pl.col(name).last().alias(f"{name}"))
            elif col["aggregate"] == 'distinct':
                agg_exprs.append(pl.col(name).n_unique().alias(f"{name}"))
            else:
                raise ValueError(f"Unknown aggregation type: {col['aggreagte']}")

        return df.groupby(group_by_column).agg(agg_exprs)
    
    def format_column_data(self, df, fmt, col):
       
        formated_df = df
        if col in df.columns:
            if df[col].dtype == pl.Date32 or df[col].dtype == pl.Date64 or df[col].dtype == pl.Datetime:
                if fmt == 'year':
                    df = df.with_column(pl.col(col).dt.year().alias(col))
                elif fmt == 'quarter':
                    df = df.with_column((pl.col(col).dt.month() // 3 + 1).alias(col))
                elif fmt == 'month':
                    df = df.with_column(pl.col(col).dt.month().alias(col))
                elif fmt == 'week':
                    df = df.with_column(pl.col(col).dt.week().alias(col))
                elif fmt == 'date':
                    df = df.with_column(pl.col(col).dt.strftime('%Y-%m-%d').alias(col))
                elif fmt == 'hours':
                    df = df.with_column(pl.col(col).dt.hour().alias(col))
                else:
                    raise ValueError(f"Unknown format option: {fmt}")
            else:
                raise ValueError(f"Column {col} is not a date or datetime type.")
        return df

    

    def general_data_pre_trasformer(self,df, column_meta, chart_type):
       
        primary_axis = column_meta.get('primary_axis', {})
        secondary_axis = column_meta.get('secondary_axis', {})
        if not isinstance(df, pl.DataFrame):
            raise ValueError("Input must be a Polars DataFrame")
        date_formated_data = df
        if primary_axis["format"]:
            date_formated_data = self.format_column_data(df=df, fmt=primary_axis["format"], col=primary_axis["column_name"])

        filtered_data = date_formated_data
        transformed_data = date_formated_data
        if primary_axis["column_name"] and primary_axis["filter_type"] and primary_axis["filter_by"]:
            filtered_data = self.filter_dataframe(df=df,column=primary_axis["column_name"], filter_type=primary_axis["filter_type"], filter_value=primary_axis["filter_by"])
        
        print(filtered_data)
        if chart_type == "pie_chart":
            transformed_data = self.aggregate_dataframe(df=filtered_data, group_by_column=secondary_axis[0]["group_by"], agg_dict=secondary_axis)
        else:
            transformed_data = self.aggregate_dataframe(df=filtered_data, group_by_column=primary_axis["column_name"], agg_dict=secondary_axis)
        
        print("---------------------- transformed_data ------------")
        print(transformed_data)
        return transformed_data

        

    def transform_to_series(self, df):
     
        if not isinstance(df, pl.DataFrame):
            raise ValueError("Input must be a Polars DataFrame")
    
        # Convert the Polars DataFrame to a dictionary
        result = {col: df[col].to_list() for col in df.columns}
        return result

    def pie_data_transformer(self, df, column_meta):

        internal_data = df
        primary_axis = column_meta.get('primary_axis', {})
        secondary_axis = column_meta.get('secondary_axis', {})
        pie_data_series = []
        
        for item in secondary_axis:
            data = []
            for index, data_point in enumerate(internal_data[item["column_name"]]):
                data.append({"name": internal_data[primary_axis["column_name"]][index], "y": data_point})
            pie_data_series.append({
                "name" : item["column_name"],
                "data": data
            })

        
        return pie_data_series
    
    def area_range_data_transformer(self, df, column_meta):
    
        internal_data = df
        primary_axis = column_meta.get('primary_axis', {})
        secondary_axis = column_meta.get('secondary_axis', {})
        pie_data_series = []
        if(len(secondary_axis) == 2):
            ads = "" 


        
        return pie_data_series
    
    def bubble_data_transformer(self, df, column_meta):
        internal_data = df
        primary_axis = column_meta.get('primary_axis', {})
        secondary_axis = column_meta.get('secondary_axis', {})
        bubble_data_series = []
        if(len(secondary_axis) == 3):
            x = internal_data[secondary_axis[0]["column_name"]]
            y = internal_data[secondary_axis[1]["column_name"]]
            z = internal_data[secondary_axis[2]["column_name"]]
            name = internal_data[primary_axis["column_name"]]
            for index, item in enumerate(name):
                bubble_data_series.append({"x": x[index], "y": y[index], "z": z[index], "name": name[index]})



        
        return [{"data": bubble_data_series}]
    
    def linear_data_transformer(self, df, column_meta, chart_type):
    
        internal_data = df
        primary_axis = column_meta.get('primary_axis', {})
        secondary_axis = column_meta.get('secondary_axis', {})
        linear_data_series = []
        
        for item in secondary_axis:
            if item["group_by"] and chart_type == "grouped_bar_graph":
                linear_data_series.append({"name": item["column_name"], "data": internal_data[item["column_name"]], "stack": item["group_by"]})
            else:
                linear_data_series.append({"name": item["column_name"], "data": internal_data[item["column_name"]]})
                
        
        categories = internal_data[primary_axis["column_name"]]
        
        return linear_data_series, categories
    
    def grouping_data_transformer(self, df, column_meta):
        
        internal_data = df
        primary_axis = column_meta.get('primary_axis', {})
        secondary_axis = column_meta.get('secondary_axis', {})
        linear_data_series = []

        padded_lists = []
        for item in secondary_axis:
            padded_lists.append(item["column_name"])
            linear_data_series.append({"name": item["column_name"], "data": [sum(internal_data[item["column_name"]])]})
        
        return linear_data_series, padded_lists
    
    def general_data_transformer(self, chart_type, df, column_meta, base_config):
        data_added_config = base_config
        if chart_type == "pie_chart":
            series = self.pie_data_transformer(df=df, column_meta=column_meta)
            data_added_config["series"] = series
        elif chart_type == "horizontal_bar_graph":
            series,categories = self.grouping_data_transformer(df=df, column_meta=column_meta)
            data_added_config["series"] = series
            data_added_config["xAxis"]["categories"] = categories
            data_added_config["xAxis"]["labels"] = {"enabled": False}
        elif chart_type == "bubble_chart":
            series = self.bubble_data_transformer(df=df, column_meta=column_meta)
            data_added_config["series"] = series
        else:
            series,categories = self.linear_data_transformer(df=df, column_meta=column_meta, chart_type=chart_type)
            data_added_config["series"] = series
            data_added_config["xAxis"]["categories"] = categories
        
        return data_added_config



    def generate_general_highcharts_config(self, chart_type, x_axis_title=None, y_axis_title=None, axis_zooming=False, axis_type="normal"):
      
         # Fetch the base configuration for the specified chart type
        specific_config = self.highcharts_configurations.get(chart_type, {}).copy()

        # Initialize with base chart configuration
        base_config = self.base_chart_config.copy()
        base_config["chart"]["type"] = specific_config.pop("type", "line")
        
        # Merge specific configurations
        for key, value in specific_config.items():
            base_config[key] = value
        
        if x_axis_title:
            base_config.setdefault('xAxis', {})['title'] = {'text': x_axis_title}
        else:
            base_config.setdefault('xAxis', {})['title'] = {'text': ""}
        if y_axis_title:
            base_config.setdefault('yAxis', {})['title'] = {'text': y_axis_title}
        else:
            base_config.setdefault('yAxis', {})['title'] = {'text': ""}
        if axis_zooming:
            base_config.setdefault('chart', {})['zoomType'] = "x"
        
        if axis_type and axis_type != "normal":
            base_config.setdefault('yAxis', {})['type'] = axis_type
            base_config.setdefault('xAxis', {})['type'] = axis_type
        
        return base_config
    
    def cast_string_array_to_int(strings):
      
        # Create a Polars Series from the list of strings
        series = pl.Series(strings)
        
        # Cast the Series to integers
        int_series = series.cast(pl.Float32)
        
        # Convert the Polars Series back to a list
        int_list = int_series.to_list()
        return int_list

    def generate_highcharts_config(self,config, data):
        chart_meta = config.get('chart_meta', {})
        column_meta = config.get('column_meta', {})
        
        x_axis_label = chart_meta.get('x_axis_label', "")
        y_axis_label = chart_meta.get('y_axis_label', "")
        axis_zooming = chart_meta.get('axis_zooming', False)
        chart_type = chart_meta.get('chart_type', "line_graph")
        axis_type = chart_meta.get('axis_type', "normal")
        series_name_enabled = chart_meta.get('series_name_enabled', False)
        pointers = chart_meta.get('pointers', True)
        
        # Fetch the base configuration for the specified chart type
        base_config = self.generate_general_highcharts_config(
            chart_type=chart_type,
            x_axis_title=x_axis_label,
            y_axis_title=y_axis_label,
            axis_zooming=axis_zooming,
            axis_type=axis_type
        )

        # internal_data =  pl.read_json("/Users/raahulprem/Documents/master/mission/ods/livedocs/vm-lib/livedocs/test.json")
        internal_data =  data

        transformed_internal_data = self.general_data_pre_trasformer(df=internal_data,column_meta=column_meta, chart_type=chart_type)
        series_internal_data = self.transform_to_series(df=transformed_internal_data)

        completed_config = self.general_data_transformer(df=series_internal_data, column_meta=column_meta,chart_type=chart_type, base_config=base_config)

        return completed_config

