![TopImage!](images/Santander_Cycles.jpg)

# **Cycle Hire Project**

---

## Contents
- [Overview](#Overview)
- [How to use](#How-to-use)
- [Exploration exaples](#Exploration-exaples)

## Overview
An ML project is being undertaken to analyze and model cycle hire data from Google Cloud Platform (GCP). \
In this project, extensive data processing and visual analytics are employed to extract valuable insights  \
from the dataset. Additionally, visually appealing figures are being generated to effectively convey these \
insights. Following the necessary preprocessing and data engineering, the data is being modeled to predict \
the demand at the top 20 busiest cycle stations. H2O AutoML is currently being utilized for this purpose. \
Nevertheless, it is acknowledged that the unique characteristics of the dataset allow for further \
experimentation and refinement.


## How to use

- Create virtual environment.
```
$ python -m venv my_venv_name
```
- Install requirements.
```
$ pip install -r requirements.txt
``` 
- Running the whole pipeline result in creating and saving files.
- Modify the directory names in the config_essencemc.yaml file.
- Run the main.ipynb file to run the whole pipeline.


## Exploration exaples

### Daily and Weekly usage

![Daily and Weekly Usage Distribution](images/Daily_and_weekly_usage_distribution.png)

### Busiest starting stations in rides per year

![Busiest starting stations in rides per year](images/Busiest_starting_stations_in_rides_per_year.png)

### Most profitable stations per year
![Most profitable stations per year](images/Most_profitable_stations_per_year.png)


