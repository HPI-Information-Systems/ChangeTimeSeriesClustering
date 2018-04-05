# ChangeTimeSeriesClustering
Scala-Spark Framework to cluster changes in data based on the data format:

<t,e,p,v> (meaning at point of time t entity e changed in property p to a new value v)

The framework works by performing the following steps:
  +grouping changes
  +filtering groups [optional]
  +aggregation of groups to time series (summing up all changes in a user-defined time window)
  +Time Series Transformation [optional]
  +Filtering Results [optional]
  +Feature Extraction [optional]
  +Clustering Algorithm
  
Each Step can be fed to the framework as user-defined functions (Scala lambdas)

For a quick start call de.hpi.data_change.time_series_similarity.Main with the arguments:
  <config.json>
  -local
  
  Where config.json is a json file containing all relevant information (sample configs are given in /src/main/resources/configs, adapt them to suit your needs)
  
  In order to customize the framework steps either supply lambda-functions to the de.hpi.data_change.time_series_similarity.Clustering instance or implement new transformation methods in de.hpi.data_change.time_series_similarity.data.TimeSeries
