import os
import time
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, Point
from google.cloud import bigquery
from ..model_development.data_3exploration import DataExplorer


class DataPreprocessor:
    """
    Access the cycle_stations table from the London Bicycle Hires data in GCP and process it.
        1. Load the Geo-spatial data of London boroughs.
           This is an external source. 
           The data is stored in a json file and located in London_geodata directory (in the root directory).
        2. Load the cycle_stations data.
        3. Iterate through the cycle_stations data and use a function to identify in which London borough each cycle station is located.
        4. Save the data in pd dataframe format in the info_tracker object.
        5. Store the processed data in a bigquery dataset.
        6. Create preview for the processed data.

    :param config: An object that reads the pipeline configurations from a yaml file and load them. Initiated at the beginning of the pipeline.              
    :param info_tracker: An object that is used throughout the pipeline to track useful information. Initiated at the beginning of the pipeline.
    :param gcp_client: A Google Cloud Platform client. Initiated at the beginning of the pipeline.
    """

    def __init__(self, config, info_tracker, gcp_client):
        self.config = config
        self.info_tracker = info_tracker
        self.__gcp_client = gcp_client

        # self.__london_geodf = self.__load_london_geodata()
        # self.__bike_stations_data = self.__load_stations_data()
        # self.info_tracker.cycle_station_data_with_borough_names = self.__add_borough_name_in_station_data()
        # self.__upload_cycle_station_data_with_borough_names_to_bigquery()
        # self.info_tracker.cycle_station_with_borough_names_preview = self.__create_cycle_station_data_with_borough_names_preview()

    def __load_london_geodata(self) -> gpd.GeoDataFrame:
        """ Load the London Geo data. """

        # Make geojson path and read London geo-data
        geojson_path = os.path.join(self.config.existing_paths.london_geodata_dir, self.config.existing_paths.london_geodata_file)
        london_geodf = gpd.read_file(geojson_path)
        
        # print(london_geodf)
        return london_geodf

    def __load_stations_data(self) -> pd.DataFrame:
        """ Load the cycle_stations data from GCP. """

        # Build query
        query_job = self.__gcp_client.query(
            f""" 
            SELECT *
            FROM bigquery-public-data.london_bicycles.{self.config.database.station_table}
            """
        )
        # Call query and extract data in pandas df
        df = query_job.result().to_dataframe()
        
        # print(df)
        return df

    def __identify_london_brough(self, given_lat: float, given_lon: float) -> str:
        """ 
        Take latitude and longitude of a specific cycle station.
        Create a geo-point.
        Iterates through the London boroughs geo-boundaries to identify the borough where the geometric point is located.
        Return the borough name
        """

        # Make a geo-point using the given latitude and longitude
        point = Point(given_lon, given_lat)

        # Init a borough name
        borough_name = "no_borough"

        # Iterate through London boroughs geo-data and design a polygon based on the spatial boundaries
        for idx in range(len(self.__london_geodf)):
            polygon = shape(self.__london_geodf.loc[idx, "geometry"])

            # Check if the polygon contains the created geo-point
            if polygon.contains(point):
                borough_name = str(self.__london_geodf.loc[idx, "name"])
        
        return borough_name
                
    def __add_borough_name_in_station_data(self):
        """ 
        Use the function identify_london_borough to identify the borough name for each given combination of lat and lon.
        Iterate through the cycle_stations table to identify the corresponding borough of each bike station.
        Add the borough name in a new column called borough_name.
        """

        # Copy the cycle_station df to make an indipendent local variable
        local_df = self.__bike_stations_data.copy()
        
        # Make a new column called borough name and fill it with the string borough_name.
        local_df["borough_name"] = "name"

        # Iterate through the station data and extract the latitude and longitude of each station
        for row in range(len(local_df)):
            lat = local_df.loc[row, "latitude"]
            lon = local_df.loc[row, "longitude"]

            # Use the identify_london_brough method to identify the corresponding borough for the given cycle station
            borough_name = self.__identify_london_brough(given_lat=lat, given_lon=lon)

            # Add the borough name in the same row, in the borough name column
            local_df.loc[row, "borough_name"] = borough_name
        
        # print(local_df)
        return local_df

    def __upload_cycle_station_data_with_borough_names_to_bigquery(self):
        """ Load the crated cycle station data with borough names to bigquery. """

        # Set a name to temporarily save the processed cycle station data
        file_name = "temp_df.csv"

        # Access the processed data from the info tracker object
        temp_df = self.info_tracker.cycle_station_data_with_borough_names

        # Save the data locally
        temp_df.to_csv(file_name)

        # Set the configurations of the uploading bigquery job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            # skip_leading_rows=1,
            autodetect=True
        )

        # Set the table id, including the project and the dataset ids
        table_id = f"{self.config.database.my_project}.{self.config.database.my_dataset}.{self.config.database.my_table}"

        # Run the bigquery uploading job
        with open(rf"{file_name}", "rb") as source_file:
            job = self.__gcp_client.load_table_from_file(source_file, table_id, job_config=job_config)

        while job.state != "DONE":
            time.sleep(10)
            job.reload()
            print(job.state)

        # Delete temporary df
        os.remove(file_name)

    def __create_cycle_station_data_with_borough_names_preview(self) -> pd.DataFrame:
        """ Create a preview of the cycle_station_data_with_borough_names table. The data is limited to 100 rows to accelerate the process. """

        # Define a name to be used in the Printer class
        name = "Cycle_station_with_borough_names_preview"
        
        # Build query
        query_job = self.__gcp_client.query(
            f""" 
            SELECT *
            FROM {self.config.database.my_project}.{self.config.database.my_dataset}.{self.config.database.my_table}
            LIMIT 100
            """
        )
        # Call query and extract data in pandas df
        df = query_job.result().to_dataframe()

        # Print outcome if show config is True
        if self.config.show_outcome.show_outcome:
            Printer().print_outcome(outcome=df, display_name=name)
    
        return df

    def explore_data(self):
        return DataExplorer(
            config=self.config,
            info_tracker=self.info_tracker,
            gcp_client=self.__gcp_client
        )

        