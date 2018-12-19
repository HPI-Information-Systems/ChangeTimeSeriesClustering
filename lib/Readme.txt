The content of this library folder must be installed to a local mvn repository for the pom to work. I used the following command:

mvn install:install-file -Dfile=javaml-0.1.5.jar -DgroupId=localJavaML -DartifactId=localJavaML -Dversion=0.1.5 -Dpackaging=jar
mvn install:install-file -Dfile=javaml-0.1.5.jar -DgroupId=monetdb -DartifactId=localMonetDB -Dversion=2.27 -Dpackaging=jar

After this, the jars should be included in the shaded jar without problems