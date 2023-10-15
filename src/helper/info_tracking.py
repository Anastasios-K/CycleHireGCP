""" Object used to track and store useful information throughout the pipeline. """

from dataclasses import dataclass
from typing import Optional
import pandas as pd


@dataclass
class InfoTracker:
    """ 
    Module used throughout the pipeline to track useful information.
    A number of info is used in later stages of the pipeline.
    """
    cycle_hire_preview: Optional[pd.DataFrame] = None
    cycle_stations_preview: Optional[pd.DataFrame] = None
    hires_null_values_count: Optional[pd.DataFrame] = None
    stations_null_values_count: Optional[pd.DataFrame] = None
    total_rides_n_duration_per_year: Optional[pd.DataFrame] = None
    busiest_starting_stations_in_rides: Optional[pd.DataFrame] = None
    least_busy_starting_stations_in_rides: Optional[pd.DataFrame] = None
    busiest_starting_stations_in_rides_per_year: Optional[pd.DataFrame] = None
    most_profitable_starting_station_per_year: Optional[pd.DataFrame] = None
    top_destinations_of_all_time: Optional[pd.DataFrame] = None
    top_destinations_per_year: Optional[pd.DataFrame] = None
    most_popular_roots_of_all_time: Optional[pd.DataFrame] = None
    top_roots_per_year: Optional[pd.DataFrame] = None
    daily_n_weekly_usage_pattern: Optional[pd.DataFrame] = None
    cycle_station_data_with_borough_names: Optional[pd.DataFrame] = None
    cycle_station_with_borough_names_preview: Optional[pd.DataFrame] = None
    total_duartion_per_borough: Optional[pd.DataFrame] = None
    h2o_leaderboard = None
    h2o_leaderboard_df: Optional[pd.DataFrame] = None
                 