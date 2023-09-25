# **Stock Price Prediction with Deep Neural Networks**
[![Maintainability](https://api.codeclimate.com/v1/badges/586edac1a63049e5299c/maintainability)](https://codeclimate.com/github/Anastasios-K/Crypto_Prise_Prediction/maintainability)

---

## Contents
- [Overview](#Overview)
- [How to use](#How-to-use)
- [The Data](#The-Data)
- [Outcome](#Outcome)
- [The Pipeline](#The-Pipeline)
  - [Data Preprocessing](#Data-Preprocessing)
  - [Data Exploration](#Data-Preprocessing)
  - [Technical Analysis Enrichment](#Technical-Analysis-Enrichment)
  - [Model Development](#Model-Development)
  - [Class Creation](#Class-Creation)
  - [Training](#Training)
  - [Testing](#Testing)
  - [Tracking](#Tracking)
- [How to Build a New model](#How-to-Build-a-New-model)
- [Relevant reading](#Relevant-reading)

## Overview
An ML pipeline for time-series (stock price) classification which is developed taking into consideration \
the main principles of Object-Oriented and Data-Oriented Programming. The current Data-Oriented Strategy \
Pattern allows users to change the behavior of the pipeline, choosing between different TensorFlow\Keras \
algorithms. This design also assures the execution of mandatory steps whereas, at the same time, it gives \
access to a number of optional functions. For more detail, look at the [Pipeline](#The-Pipeline) section.

## How to use

#### <span style="color:#00A3E0">1. Run it without using the Docker file</span>

- Create virtual environment.
```
$ python -m venv my_venv_name
```
- Install requirements.
```
$ pip install -r requirements.txt
``` 
- Modify data_path parameter, in the path field of the 
  [config-file](https://github.com/Anastasios-K/Stock_Price_Prediction_with_Bi-Directional_LSTM/blob/main/src/config/config.yaml), \
  if the data is not located in the default data folder or if a different dataset is in use.
  
- Run main.py through the command line.
```
$ python main.py
```
  
#### <span style="color:#00A3E0">2. Run it through the Docker file</span>


## The Data
The default dataset is the Tesco plc daily stock price. \
Other datasets can be used, if they have the following features. \
Date, Close, Open, High, Low, Volume

## Outcome
Running the main.py creates the following: \
The folder named "mlruns" and a subfolder that correspond to the specific trial/run. \
The folder named "results" and the subfolders "best_models", "figures", "hyperparams" and "models".

## The Pipeline
The ML Pipeline icludes the following steps. Some of them are manadatory and some are optional. \
For more details look at the corresponding field below.

### Data Preprocessing
***Mandatory step*** \
It is automatically executed when the Pipeline object is created and it does the follwing:

- Convert data to the desirable format and fix data types.
- Remove unused data features.
- Fill NaN values and stores their amount in the info tracking object.
- Detect duplicates based on the Date feature, store their amount in the info tracking object \
  and remove them form the data
  
### Data Exploration
***Optional step*** \
The users have to call the corresponding methods from the Pipeline object and can do the following: \
Look at the [main.py](https://github.com/Anastasios-K/Stock_Price_Prediction_with_Bi-Directional_LSTM/blob/main/main.py) 
for a sample run.

- Create and save an Explanatory Data Analysis report.
- Create and save interactive plot of multiple data resolutions.
- Calculate correlation and save interactive plot.
- Calculate autocorrelation and partial autocorrelation and save plots.
- Plot distribution of each data feature and save plot.

### Technical Analysis Enrichment
***Optional step*** \
The users have to call the corresponding method from the Pipeline object and define the feature\s that they want to use.

- Enrich the data with Technical Analysis (from finance) features
- Users can choose any combination of the following features \
  [simple moving average](https://www.investopedia.com/terms/s/sma.asp) \
  [exponential moving avarerage](https://www.investopedia.com/terms/e/ema.asp) \
  [moving average convergence divergence](https://www.investopedia.com/terms/m/macd.asp) \
  [money flow index](https://www.investopedia.com/terms/m/mfi.asp)
  
### Model Development
***Mandatoty step*** \
The users have to initiate a model object and pass it to the pipeline. \
At the moment, an LSTM class is availbale only. \
A Bi-Directional LSTM class is coming soon. \
Look at the field [How to build a New model](#How-to-Build-a-New-model) if you want to build a new model class.

### Class Creation
***Mandatoty step*** \
This is a separate object which is automatically initiated and executed during the model developement. \
This step creates the target labels for the training and testing sets. \
The labels are created based on a specific strategy that depends on the tolerance factor set in the 
[config-file](https://github.com/Anastasios-K/Stock_Price_Prediction_with_Bi-Directional_LSTM/blob/main/src/config/config.yaml). \
\
If the tolerance equals to zero (0): \
Then two classes 0 and 1 are created, leading to a binary classification. \
\
Else if the tolerance is higher than zero (0), \
Then three classes 0, 1 and 2 are created, leading to a multiclass classification.

### Training
***Mandatoty step*** \
This step includes the training, validation and hyper-parameter tuning processes. \
- [Keras-Tuner](https://keras.io/keras_tuner/) is used for the tuning process.
- A [TensorBoard](https://www.tensorflow.org/api_docs/python/tf/keras/callbacks/TensorBoard)
  callback is creates a tracking process at the end of each training step.
- An [Early-Stopping](https://www.tensorflow.org/api_docs/python/tf/keras/callbacks/EarlyStopping)
  callback prevents overfitting, monitoring validation loss at the end of each epoch.
- A [ModelCheckpoint](https://www.tensorflow.org/api_docs/python/tf/keras/callbacks/ModelCheckpoint) 
  callback store the training progress, at the end of each training step. \
  So, users can restore trainig process at any point.

### Testing
***Mandatoty step*** \
This step includes the testing process only. \
- Extract and save the five best model from the trained Keras hypermodel.
- Predict the test set labels using the TOP 1 model.
- Calculate the following metrics: \
  [Precision](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.precision_score.html) \
  [Average Precision](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.average_precision_score.html) \
  [Recall](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.recall_score.html) \
  [F1-score](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.f1_score.html) \
  True Positive \
  True Negative \
  False Positive \
  False Negative
  
### Tracking
***Mandatoty step*** \
This step is to track the performance of the whole trial/run. \
It uses MLFlow and saves the following:
- The general parameters of the trial (look at the config-file)
- The hyper-parameters of the best models (look at the config-file)
- All the information which is tracked by the InfoTracker object, throughout the pipeline.

## How to Build a New model

## Relevant reading


