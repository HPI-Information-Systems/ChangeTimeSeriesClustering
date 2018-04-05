# ChangeTimeSeriesClustering
Scala-Spark Framework to cluster changes in data based on the data format:

<t,e,p,v> (meaning at point of time t entity e changed in property p to a new value v)

### Framework Basics

The framework consists of the following steps:
* grouping changes
* filtering groups (optional)
* aggregation of groups to time series (summing up all changes in a user-defined time window)
* time series transformation (optional)
* filtering resulting time series (optional)
* feature extraction (optional)
* clustering the times series (currently supports KMeans and DBA-KMeans)
  
Each Step can be fed to the framework as user-defined functions (Scala lambdas)

### Building and Execution
The library dependency javaml is required to calculate the DTW distance. Since it is not present in a central maven repository it is included in [/lib](/lib) and needs to be added to a local maven repository. Also see the[Readme](/lib/Readme.txt)in the lib directory.

For a quick start call the[Main-Class](/src/main/scala/de/hpi/data_change/time_series_similarity/Main.scala) with the arguments:

  <config.json>
  -local
  
Where config.json is a json file containing all relevant information (sample configs are given[here](/src/main/resources/configs),adapt them to suit your needs)

In order to build a jar that is executable on a cluster execute mvn package (the pom is already configured to build a fat jar)
  
### Customization    
  In order to customize the framework steps either supply lambda-functions to the[Clustering-instance](/src/main/scala/de/hpi/data_change/time_series_similarity/Clustering.scala)instance or implement new transformation methods in the[Time Series Class](/src/main/scala/de/hpi/data_change/time_series_similarity/data/TimeSeries.scala)
