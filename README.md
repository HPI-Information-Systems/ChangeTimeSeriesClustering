# ChangeTimeSeriesClustering
Scala-Spark Framework to cluster changes in data based on the data format:

<t,e,p,v> (meaning at point of time t entity e changed in property p to a new value v)

### Framework Basics

The framework consists of the following steps:
* Read of initial query into a spark dataframe. The combination of all columns but the last one will be interpreted as a grouping key, which defines the time series object. The last column is expected to be a timestamp.
* transformation of each group to a time series (summing up all changes in a user-defined time window)
* time series transformation (optional)
* feature extraction (optional)
* clustering the times series (currently supports KMeans and DBA-KMeans)
  

### Building and Execution

For a quick start call the [Main-Class](/src/main/scala/de/hpi/data_change/time_series_similarity/Main.scala) with the arguments:

  <config.json>
  -local (second parameter only if running on a local machine)
  
Where config.json is a json file containing all relevant information (a sample config is given [here](/src/main/resources/localConfigs/sampleConfig.json),adapt them to suit your needs)

In order to build a jar that is executable on a cluster execute mvn package (the pom is already configured to build a fat jar) and copy the larger of the two jars to the server (it contains all necessary dependencies)
    
