""" Data exploration. """

import os
# import folium
import pandas as pd
# import geopandas as gpd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
# from branca.colormap import linear
from ..model_development.data_engineering import DataEngineer


class DataExplorer:
    """
    Data exploration.
        1. Total number of rides and riding duration per year.
        2. Busiest starting stations in rides of all time.
        3. Least busy starting stations in rides of all time.
        4. Busiest starting stations in rides per year.
        5. Most profitable stations per year.
        6. Top destinations of all time.
        7. Top destinations per year.
        8. Most popular roots of all time.
        9. Most popular roots per year.
        10. Daily and weekly usage pattern.
        11. Total riding duration per borough.
    """

    def __init__(self, config, info_tracker, gcp_client):
        self.config = config
        self.info_tracker = info_tracker
        self.__gcp_client = gcp_client

        self.info_tracker.total_rides_n_duration_per_year = self.__calc_total_rides_n_duration_per_year()
        self.__plot_total_rides_n_duration_per_year()
        self.info_tracker.busiest_starting_stations_in_rides = self.__identify_busiest_starting_stations_in_rides_of_all_time()
        self.__plot_busiest_starting_stations_in_rides_of_all_time()
        self.info_tracker.least_busy_starting_stations_in_rides = self.__identify_least_busy_starting_stations_in_rides_of_all_time()
        self.__plot_least_busy_starting_stations_in_rides_of_all_time()
        self.info_tracker.busiest_starting_stations_in_rides_per_year = self.__identify_busiest_starting_stations_in_rides_per_year()
        self.__plot_busiest_starting_stations_in_rides_per_year()
        self.info_tracker.most_profitable_starting_station_per_year = self.__identify_most_profitable_stations_per_year()
        self.__plot_most_profitable_stations_per_year()
        self.info_tracker.top_destinations = self.__identify_top_destinations_of_all_time()
        self.__plot_top_destinations_of_all_time()
        self.info_tracker.top_destinations_per_year = self.__identify_top_destinations_per_year()
        self.__plot_top_destinations_per_year()
        self.info_tracker.top_roots_of_all_time = self.__identify_the_most_popular_roots_of_all_time()
        self.info_tracker.top_roots_per_year = self.__identify_the_most_popular_roots_per_year()
        self.info_tracker.daily_n_weekly_usage_pattern = self.__identify_daily_n_weekly_usage_pattern()
        self.__plot_the_daily_n_weekly_usage()       
        self.info_tracker.total_duartion_per_borough = self.__create_cycle_hire_data_with_london_borough_name()
        # self.__make_an_interactive_london_map_with_boroughs_n_riding_duration()

    def __calc_total_rides_n_duration_per_year(self) -> pd.DataFrame:
        """
        Count total rides for each year (using ride id).
        Calculate total ride duration in hours for each year.
        """
        
        # Build query
        query_job = self.__gcp_client.query(
            f""" 
            SELECT 
                EXTRACT(YEAR FROM start_date) AS year,
                COUNT(rental_id) AS number_of_rides,
                ROUND(SUM(duration / 3600), 2) AS riding_duration_in_hours
            FROM 
                bigquery-public-data.london_bicycles.{self.config.database.hire_table}
            WHERE
                start_date IS NOT NULL
            GROUP BY 
                year
            ORDER BY 
                year;
            """
        )
        df = query_job.result().to_dataframe()            
        return df

    def __plot_total_rides_n_duration_per_year(self):
        """ Create an interactive line graph to show the number of bikes, number of rides and riding duration per year. """
        
        name = "Total_number_of_rides_and_riding_duration_per_year"

        # Get data from info tracker.
        data = self.info_tracker.total_rides_n_duration_per_year

        # Create figure with secondary y-axis.
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Add ride trace.
        fig.add_trace(
            go.Scatter(
                x=data.year,
                y=data.number_of_rides,
                mode="lines",
                line=dict(
                    color=self.config.plotdefault.colour1,
                    width=2,
                    dash="solid"
                ),
                name="Number of rides"
            ),
            secondary_y=False
        )

        # Add durattion trace.
        fig.add_trace(
            go.Scatter(
                x=data.year,
                y=data.riding_duration_in_hours,
                mode="lines",
                line=dict(
                    color=self.config.plotdefault.colour2,
                    width=2,
                    dash="solid"
                ),
                name="Riding duration",
            ),
            secondary_y=True
        )

        # Add figure title.
        fig.update_layout(
            title_text=name.replace("_", " "),
            title_x = 0.5
        )
        # Add x-axis title.
        fig.update_xaxes(
            title_text="Years"
        )
        # Add y-axis and secondary y-axis titles.
        fig.update_yaxes(title_text="Number of rides", secondary_y=False)
        fig.update_yaxes(title_text="Riding duration in hours", secondary_y=True)

        # Remove background grid.
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False, secondary_y=False)
        fig.update_yaxes(showgrid=False, secondary_y=True)

        # Save figure.
        fig.write_html(os.path.join(self.config.paths2create.plots_path, name + ".html"))

    def __identify_busiest_starting_stations_in_rides_of_all_time(self) -> pd.DataFrame:
        """ Identify the 3 busiest starting bike stations for the whole period. """

        # Build query.
        query_job = self.__gcp_client.query(
            f"""
            SELECT
                start_station_name AS busiest_starting_station,
                COUNT(rental_id) AS number_of_rides,
            FROM 
                bigquery-public-data.london_bicycles.{self.config.database.hire_table}
            GROUP BY 
                start_station_name
            ORDER BY 
                number_of_rides DESC
            LIMIT 3;
            """
        )
        df = query_job.result().to_dataframe()   
        return df

    def __plot_busiest_starting_stations_in_rides_of_all_time(self):
        """ Create an interactive barchart to show the busiest starting stations in rides of all time. """
        name = "Busiest_starting_stations_of_all_time"

        # Get data from info tracker.
        data = self.info_tracker.busiest_starting_stations_in_rides
        
        # Create a station trace.
        station_trace = go.Bar(
            x=data.busiest_starting_station,
            y=data.number_of_rides,
            marker=dict(color=self.config.plotdefault.colour1),
        )
        
        # Create a layout for the chart
        layout = go.Layout(
            title=dict(
                text=name.replace("_", " "),
                x=0.5
            ),
            xaxis=dict(title="Busiest stations"),
            yaxis=dict(title="Number of rides"),
        )
        
        # Create a Figure object
        fig = go.Figure(data=station_trace, layout=layout)

        # Add number of rides values inside the columns.
        fig.update_traces(texttemplate='%{y:.1f}', textposition='inside')
        
        # Save figure.
        fig.write_html(os.path.join(self.config.paths2create.plots_path, name + ".html"))

    def __identify_least_busy_starting_stations_in_rides_of_all_time(self) -> pd.DataFrame:
        """ Identify the 3 least busy starting bike stations for the whole period. """

        # Build query.
        # Use a minimum threshold of 100 rides to excluse test stations.
        # The query was tested without threshold bu the insight extracted was not very useful.
        query_job = self.__gcp_client.query(
            f"""
            SELECT
                start_station_name AS least_busy_starting_station,
                COUNT(rental_id) AS number_of_rides,
            FROM 
                bigquery-public-data.london_bicycles.{self.config.database.hire_table}
            GROUP BY 
                start_station_name
            HAVING 
                number_of_rides > 1000
            ORDER BY 
                number_of_rides
            LIMIT 3;
            """
        )
        df = query_job.result().to_dataframe()
        return df

    def __plot_least_busy_starting_stations_in_rides_of_all_time(self):
        """ Create an interactive barchart to show the least busy starting stations in rides of all time. """
        name = "Least_busy_starting_stations_of_all_time"

        # Get data from info tracker.
        data = self.info_tracker.least_busy_starting_stations_in_rides
        
        # Create a station trace.
        station_trace = go.Bar(
            x=data.least_busy_starting_station,
            y=data.number_of_rides,
            marker=dict(color=self.config.plotdefault.colour1),
        )
        
        # Create a layout for the chart
        layout = go.Layout(
            title=dict(
                text=name.replace("_", " "),
                x=0.5
            ),
            xaxis=dict(title="Least busy stations"),
            yaxis=dict(title="Number of rides"),
        )
        
        # Create a Figure object
        fig = go.Figure(data=station_trace, layout=layout)

        # Add number of rides values inside the columns.
        fig.update_traces(texttemplate='%{y:.1f}', textposition='inside')
        
        # Save figure.
        fig.write_html(os.path.join(self.config.paths2create.plots_path, name + ".html"))

    def __identify_busiest_starting_stations_in_rides_per_year(self) -> pd.DataFrame:
        """ Identify the top 3 starting stations with the highest number of rental_ids for each year. """
        
        # Build query.
        query_job = self.__gcp_client.query(
            f"""
            WITH RentalData AS (
                SELECT
                    EXTRACT(YEAR FROM start_date) AS year,
                    start_station_name,
                    COUNT(rental_id) AS number_of_rides,
                FROM
                    bigquery-public-data.london_bicycles.{self.config.database.hire_table}
                GROUP BY
                    year,
                    start_station_name
            )
            
            SELECT
                year,
                start_station_name,
                number_of_rides
            FROM(
                SELECT
                    year,
                    start_station_name,
                    number_of_rides,
                    ROW_NUMBER() OVER(PARTITION BY year ORDER BY number_of_rides DESC) AS row_num
                FROM 
                    RentalData
            )
            WHERE
              row_num <= 3
            ORDER BY
              year,
              number_of_rides DESC;
            """
        )
        df = query_job.result().to_dataframe()
        return df

    def __plot_busiest_starting_stations_in_rides_per_year(self):
        """ Create an interactive bar chart with multiple categories to show the busiest_starting_stations_in_rides_per_year. """
        name = "Busiest_starting_stations_in_rides_per_year"

        # Get data from info tracker.
        data = self.info_tracker.busiest_starting_stations_in_rides_per_year

        # Create bar chart.
        fig = px.bar(
            data,
            x='year',
            y='number_of_rides',
            color='start_station_name',
            labels={'number_of_rides': 'Number of Rides'}    
        )
        
        # Update the layout.
        fig.update_layout(
            xaxis=dict(title='Years'),
            yaxis=dict(title='Number of Rides'),
            legend_title='Stations',
            title=dict(
                text=name.replace("_", " "),
                x=0.5
            )        
        )

        # Add number of rides values inside the columns.
        fig.update_traces(texttemplate='%{y:.1f}', textposition='inside')

        # Add number of rides values inside the columns.
        fig.update_traces(texttemplate='%{y:.1f}', textposition='inside')
        
        # Save figure.
        fig.write_html(os.path.join(self.config.paths2create.plots_path, name + ".html"))
        
    def __identify_most_profitable_stations_per_year(self) -> pd.DataFrame:
        """ 
        Identify the top 3 starting stations with the highest profitability.
        Profitability Calculation:
        1. Consider a rate of £1.65 for each subsequent 30-minute period of bike usage following the initial 30 minutes from the start of the rental. 
            The pricing policy described above pertains to the current basic subscription for Santander bikes in London.
        2. Subtract 1800 seconds (equivalent to half an hour) from the duration column (riding measured in seconds),
            to exclude the initial 30 minutes of free riding offered for each ride within the first 24 hours of subscription.
            Extra charge is added after the first 30 minutes of riding.
        3. The above mentioned charge varies depending on yearly and premium subscripition.
            However, for this analysis, we focus on the standard/basic subscription due to the lack of specific subscription data.
        4. To calculate the extra 30 mins chargable periods, the remaining time is devided my 1800 and the results is stored in the extra_time column.
            So, the extra_time filled in with a float greater than 0 that shows the number of extra 30 minute riding periods.
        5. Calculate the extra profitability by multiplying the "extra_time" factor by the price per 30 minutes (£1.65).
        6. The calculated profitability does not include the initial subscription fee.
        """

        # Build query.
        query_job = self.__gcp_client.query(
            f"""
            WITH ChargableTime AS (
                SELECT
                    EXTRACT(YEAR FROM start_date) AS year,
                    start_station_name,
                    SUM(CEIL(GREATEST(ROUND((duration - 1800) / 1800, 2), 0))) AS extra_time
                FROM
                    bigquery-public-data.london_bicycles.{self.config.database.hire_table}
                GROUP BY
                    year,
                    start_station_name
            ),

            ProfitData AS (
                SELECT
                    year,
                    start_station_name,
                    ROUND(SUM(extra_time * 1.65), 2) AS profit
                FROM
                    ChargableTime
                GROUP BY
                    year,
                    start_station_name
            )

            SELECT
                year,
                start_station_name,
                profit
            FROM(
                SELECT
                    year,
                    start_station_name,
                    profit,
                    ROW_NUMBER() OVER(PARTITION BY year ORDER BY profit DESC) AS row_num
                FROM 
                    ProfitData
            )
            WHERE
                row_num <= 3
            ORDER BY
                year,
                profit DESC;
            """
        )
        df = query_job.result().to_dataframe()
        return df

    def __plot_most_profitable_stations_per_year(self):
        """ Create an interactive bar chart with multiple categories to show the most_profitable_stations_per_year. """
        name = "Most_profitable_stations_per_year"

        # Get data from info tracker.
        data = self.info_tracker.most_profitable_starting_station_per_year

        # Create bar chart.
        fig = px.bar(
            data,
            x='year',
            y='profit',
            color='start_station_name',
            labels={'profit': 'Profit £'}    
        )
        
        # Update the layout.
        fig.update_layout(
            xaxis=dict(title='Years'),
            yaxis=dict(title='Profit in £'),
            legend_title='Stations',
            title=dict(
                text=name.replace("_", " "),
                x=0.5
            )        
        )

        # Add number of rides values inside the columns.
        fig.update_traces(texttemplate='%{y:.1f}', textposition='inside')
        
        # Save figure.
        fig.write_html(os.path.join(self.config.paths2create.plots_path, name + ".html"))

    def __identify_top_destinations_of_all_time(self) -> pd.DataFrame:
        """ Identify the 3 top destinations of all time. """
        
        # Build query.
        query_job = self.__gcp_client.query(
            f"""
            SELECT
                end_station_name AS top_destinations,
                COUNT(rental_id) AS number_of_rides,
            FROM 
                bigquery-public-data.london_bicycles.{self.config.database.hire_table}
            GROUP BY 
                top_destinations
            ORDER BY 
                number_of_rides DESC
            LIMIT 3;
            """
        )
        df = query_job.result().to_dataframe()
        return df

    def __plot_top_destinations_of_all_time(self):
        """ Create an interactive barchart to show the top destinations of all time. """
        name = "Top_destinations_of_all_time"

        # Get data from info tracker.
        data = self.info_tracker.top_destinations
        
        # Create a station trace.
        station_trace = go.Bar(
            x=data.top_destinations,
            y=data.number_of_rides,
            marker=dict(color=self.config.plotdefault.colour1),
        )
        
        # Create a layout for the chart
        layout = go.Layout(
            title=dict(
                text=name.replace("_", " "),
                x=0.5
            ),
            xaxis=dict(title="Top destinations"),
            yaxis=dict(title="Number of rides"),
        )
        
        # Create a Figure object
        fig = go.Figure(data=station_trace, layout=layout)

        # Add number of rides values inside the columns.
        fig.update_traces(texttemplate='%{y:.1f}', textposition='inside')
        
        # Save figure.
        fig.write_html(os.path.join(self.config.paths2create.plots_path, name + ".html"))

    def __identify_top_destinations_per_year(self) -> pd.DataFrame:
        """ Identify the 3 top destinations per year. """

        # Build query.
        query_job = self.__gcp_client.query(
            f"""
            WITH TopDest AS (
                SELECT
                    EXTRACT(YEAR FROM start_date) AS year,
                    end_station_name AS top_destinations,
                    COUNT(rental_id) AS number_of_rides,
                FROM
                    bigquery-public-data.london_bicycles.{self.config.database.hire_table}
                GROUP BY
                    year,
                    top_destinations
                )
                
                SELECT
                    year,
                    top_destinations,
                    number_of_rides
                FROM(
                    SELECT
                        year,
                        top_destinations,
                        number_of_rides,
                        ROW_NUMBER() OVER(PARTITION BY year ORDER BY number_of_rides DESC) AS row_num
                    FROM 
                        TopDest
                )
                WHERE
                    row_num <= 3
                ORDER BY
                    year,
                    number_of_rides DESC;
                """
        )
        df = query_job.result().to_dataframe()
        return df

    def __plot_top_destinations_per_year(self):
        """ Create an interactive bar chart with multiple categories to show the top destinations per year. """
        name = "Top_destinations_per_year"

        # Get data from info tracker.
        data = self.info_tracker.top_destinations_per_year

        # Create bar chart.
        fig = px.bar(
            data,
            x='year',
            y='number_of_rides',
            color='top_destinations',
            labels={'number_of_rides': 'Number of Rides'}    
        )
        
        # Update the layout.
        fig.update_layout(
            xaxis=dict(title='Years'),
            yaxis=dict(title='Number of Rides'),
            legend_title='Top destinations',
            title=dict(
                text=name.replace("_", " "),
                x=0.5
            )        
        )

        # Add number of rides values inside the columns.
        fig.update_traces(texttemplate='%{y:.1f}', textposition='inside')
        
        # Save figure.
        fig.write_html(os.path.join(self.config.paths2create.plots_path, name + ".html"))

    def __identify_the_most_popular_roots_of_all_time(self) -> pd.DataFrame:
        """ Identify the 20 most popular roots of all time. """

        # Build query.
        query_job = self.__gcp_client.query(
            f"""
            SELECT 
                start_station_name, 
                end_station_name, 
                COUNT(*) AS frequency
            FROM 
                bigquery-public-data.london_bicycles.{self.config.database.hire_table}
            GROUP BY 
                start_station_name, end_station_name
            ORDER BY 
                frequency DESC
            LIMIT 20;
            """
        )
        df = query_job.result().to_dataframe()
        return df

    def __identify_the_most_popular_roots_per_year(self) -> pd.DataFrame:
        """ Identify the 3 most popular roots per year. """

        # Build query.
        query_job = self.__gcp_client.query(
            f"""
            WITH PopularRoots AS (
                SELECT
                    EXTRACT(YEAR FROM start_date) AS year,
                    start_station_name, 
                    end_station_name, 
                    COUNT(*) AS number_of_routes
                FROM
                    bigquery-public-data.london_bicycles.{self.config.database.hire_table}
                GROUP BY
                    year,
                    start_station_name, 
                    end_station_name
            )
            
            SELECT
                year,
                start_station_name,
                end_station_name,
                number_of_routes
            FROM(
                SELECT
                    year,
                    start_station_name,
                    end_station_name,
                    number_of_routes,
                    ROW_NUMBER() OVER(PARTITION BY year ORDER BY number_of_routes DESC) AS row_num
                FROM 
                    PopularRoots
            )
            WHERE
                row_num <= 3
            ORDER BY
                year,
                number_of_routes DESC;
            """
        )
        df = query_job.result().to_dataframe()
        return df

    def __identify_daily_n_weekly_usage_pattern(self) -> pd.DataFrame:
        """ Identify bike usage daily and weekly patterns. """

        # Build query.
        query_job = self.__gcp_client.query(
            f"""
            SELECT
                EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,
                EXTRACT(HOUR FROM start_date) AS hour_of_day,
                ROUND(SUM(duration / 3600), 2) AS total_duration
            FROM
                bigquery-public-data.london_bicycles.{self.config.database.hire_table}
            GROUP BY
                day_of_week,
                hour_of_day
            ORDER BY
                day_of_week,
                hour_of_day;
            """
        )
        df = query_job.result().to_dataframe()

        # Pivot the DataFrame.
        pivot_df = df.pivot(index='day_of_week', columns='hour_of_day', values='total_duration')

        # Reorder columns to have hours in ascending order.
        pivot_df = pivot_df.reindex(sorted(pivot_df.columns), axis=1)

        # Rename index.
        day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        pivot_df.index = [day_names[int(day) - 1] for day in pivot_df.index]

        return pivot_df

    def __plot_the_daily_n_weekly_usage(self):
        """ Create an interactive heatmap to plot the daily and weekly usage distribution. """
        name = "Daily_and_weekly_usage_distribution"
        
        pivot_data = self.info_tracker.daily_n_weekly_usage_pattern
        
        # Create heatmap trace.
        heatmap_trace = go.Heatmap(
            z=np.array(pivot_data),  
            x=pivot_data.columns,
            y=pivot_data.index,
            colorscale=self.config.plotdefault.cmap,
            colorbar=dict(
                title="Total Duration in hours",
                titleside="top"
            )
        )
        # Create layout.
        layout = go.Layout(
            title=dict(
                font=dict(
                    color=self.config.plotdefault.title_color,
                    family=self.config.plotdefault.title_font_style,
                    size=self.config.plotdefault.title_font_size
                ),
                text=name.replace("_", " "),
                x=0.5  # To place the title at the middle
            ),
            xaxis_title=dict(text="Hour of day"),
            yaxis_title=dict(text="Day of week"),
            xaxis = dict(tickvals =np.arange(0, 24, 1))
        )
        # Compose final figure.
        fig = go.Figure(
            data=heatmap_trace,
            layout=layout
        )
        
        # Save figure.
        fig.write_html(os.path.join(self.config.paths2create.plots_path, name + ".html"))

    def __create_cycle_hire_data_with_london_borough_name(self):
        """
        Join cycle hire table and the processed station table created and saved by the preproc_data class,
        to create a cycle hire table with London borough names.
        The borough names are inserted based on the start_station_id of cycle_hire table.
        The goal is to identify the boroughs with the highest riding duration.
        So, we sum the duration duration is also divided by 3600 to be converted in hours and we group by the borough name.
        """

        # Build query
        query_job = self.__gcp_client.query(
            f"""
            SELECT 
                station_table.borough_name,
                ROUND(SUM(hire_table.duration / 3600), 2) AS total_duration_in_hours
            FROM
                bigquery-public-data.london_bicycles.{self.config.database.hire_table} AS hire_table            
            LEFT JOIN 
                {self.config.database.my_project}.{self.config.database.my_dataset}.{self.config.database.my_table} AS station_table 
            ON 
                hire_table.start_station_id = station_table.id
            WHERE 
                hire_table.start_station_id IS NOT NULL
            GROUP BY 
                station_table.borough_name;
            """
        )
        
        df = query_job.result().to_dataframe()
        return df

    # def __make_an_interactive_london_map_with_boroughs_n_riding_duration(self):
    #     """ xxx """

    #     # Get data from info tracker
    #     duration_data = self.info_tracker.total_duartion_per_borough

    #     # Load London geo data
    #     geojson_path = os.path.join(self.config.paths.london_geodata_dir, self.config.paths.london_geodata_file)
    #     gdf = gpd.read_file(geojson_path)
    #     # Convert the geo-df into pandas df
    #     pd_gdf = pd.DataFrame(gdf)

    #     # Merge the duration df and the London geo-df
    #     # And fill missing values with 0
    #     merged_df = pd_gdf.merge(duration_data, how="left", left_on="name", right_on="borough_name")
    #     merged_df["total_duration_in_hours"].fillna(0, inplace=True)
    #     merged_df.drop(columns=["borough_name"], inplace=True)
    #     merged_gdf = gpd.GeoDataFrame(merged_df)        

    #     # Create a colourmap considering the total_duration values
    #     colormap = linear.YlGnBu_09.scale(
    #         merged_gdf["total_duration_in_hours"].min(), 
    #         merged_gdf["total_duration_in_hours"].max()
    #     )
    #     colormap.caption = "Total duration color map"

    #     # Calculate the bounding box of the London boroughs
    #     bbox = merged_gdf.total_bounds
        
    #     # Create a blank map with a transparent background
    #     m = folium.Map(
    #         location=[(bbox[1] + bbox[3]) / 2, (bbox[0] + bbox[2]) / 2],
    #         zoom_start=12,
    #         tiles=None,
    #     )

    #     # Define a colourmap function
    #     def color_mapper(feature):
    #         return colormap(feature["properties"]["total_duration_in_hours"])

    #     # Add GeoJSON data for London boroughs to the map with colors based on total_duration_in_hours
    #     folium.GeoJson(
    #         merged_gdf,
    #         name="London Boroughs",
    #         style_function=lambda x: {
    #             "fillColor": color_mapper(x),
    #             "color": 'black',
    #             "weight": 2,
    #             "fillOpacity": 0.7,
    #         },
    #         highlight_function=lambda x: {
    #             "weight": 5,
    #             "color": "black",
    #         },
    #         tooltip=folium.GeoJsonTooltip(
    #             fields=["name", "total_duration_in_hours"], 
    #             aliases=["Borough", "Total riding duration (hours)"], 
    #             labels=True
    #         ),
    #     ).add_to(m)

    #     # Add the colorbar to the map
    #     colormap.add_to(m)
        
    #     # Add borough names as markers
    #     for index, row in gdf.iterrows():
    #         centroid = row["geometry"].centroid
    #         folium.Marker(
    #             location=[centroid.y, centroid.x],
    #             icon=folium.DivIcon(
    #                 html=f"<div style='font-size: 6pt; font-weight: bold;'>{row['name']}</div>",
    #                 icon_size=(50, 50),
    #             ),
    #             popup=row["name"],
    #         ).add_to(m)
        
    #     # Save the map to an HTML file
    #     m.save(os.path.join(self.config.paths.plots_path, "london_boroughs_colored_by_extra_data_matplotlib_cmap_map3.html"))
        
    def prepare_data_for_modelling(self):
        return DataEngineer(
            config=self.config,
            info_tracker=self.info_tracker,
            gcp_client=self.__gcp_client
        )