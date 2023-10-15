""" Data Engineering class. """

import os
from ydata_profiling import ProfileReport
# from ..model_development.s5_build_model_train_n_test import ModelBuilderTrainerTester


class DataEngineer:

    def __init__(self, config, info_tracker, gcp_client):
        """
        Extract data for modelling and run data engineering for the specific dataset only.

        Modelling Goal:
            To predict the demand for the 20 busiest start_stations of all time.

        The created dataset includes the following attributes:
            start_station_id
            year
            month
            day
            hour
            rental_count (this attribute is removed at the end of engineering process)
            labels (the labels are created based on the rental count)

        An Exploratory Data Analysis report is created and saved for the specific dataset.
        """
        
        self.config = config
        self.info_tracker = info_tracker
        self.__gcp_client = gcp_client

        self.__data_for_modelling = self.__extract_data_for_modelling()
        self.__plot_rental_count_distribution()
        self.__bin_rental_count_to_create_classes()
        self.__remove_rental_count_attribute()
        self.__create_eda_report_for_modelling_data()

    @property
    def data_for_modelling(self):
        return self.__data_for_modelling 

    # Build query
    def __extract_data_for_modelling(self):
        """ Extract the cycle_data only for the 20 busiest stations. """

        query_job = self.__gcp_client.query(
            f"""
            WITH Top20Stations AS (
                SELECT *
                FROM 
                    bigquery-public-data.london_bicycles.{self.config.database.hire_table}
                WHERE 
                    start_station_id 
                IN (
                    SELECT start_station_id
                    FROM bigquery-public-data.london_bicycles.{self.config.database.hire_table}
                    WHERE start_station_id IS NOT NULL
                    GROUP BY start_station_id
                    ORDER BY COUNT(rental_id) DESC
                    LIMIT 20
                )
            )
            SELECT
                start_station_id,
                EXTRACT(YEAR FROM start_date) AS year,
                EXTRACT(MONTH FROM start_date) AS month,
                EXTRACT(DAY FROM start_date) AS day,
                EXTRACT(HOUR FROM start_date) AS hour,
                COUNT(rental_id) AS rental_count
            FROM 
                Top20Stations
            GROUP BY
                start_station_id, year, month, day, hour
            ORDER BY
                year, month, day, hour, rental_count
            """
        )

        return query_job.result().to_dataframe()

    def __plot_rental_count_distribution(self):
        """ Plot the distribution of the rental_count attribute. """
        self.data_for_modelling.rental_count.plot.hist()

    def __bin_rental_count_to_create_classes(self):
        """ 
        Create classes based on the rental_count attribute.
        If the rental_count is lower than 20, then class is 0 (low demand)
        If the rental_count is higher than 20 and lower or equal to 40, then class is 1 (medium demand)
        If the rental_count is higher than 40, then class is 0 (high demand)
        """
        self.data_for_modelling["labels"] = 0
        
        self.data_for_modelling.loc[
            (self.data_for_modelling.rental_count > 20) &
            (self.data_for_modelling.rental_count <= 40), "labels"
        ] = 1
        self.data_for_modelling.loc[
            self.data_for_modelling.rental_count > 40, "labels"
        ] = 2

    def __remove_rental_count_attribute(self):
        """ Remove the rental_count attribute. The created labels are used instead. """
        self.data_for_modelling.drop(
            columns="rental_count",
            inplace=True
        )

    def __create_eda_report_for_modelling_data(self):
        """ Create an Exploratory Data Analysis report. """

        # Prepare EDA report
        profile = ProfileReport(
            self.data_for_modelling
        )

        # Save EDA report
        profile.to_file(os.path.join(
            self.config.paths2create.eda_report,
            "eda_report.html"
        ))

    # def build_ml_model_train_n_test(self):
    #     return ModelBuilderTrainerTester(
    #         config=self.config,
    #         info_tracker=self.info_tracker,
    #         data=self.data_for_modelling
    #     )
       