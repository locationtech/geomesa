# Spark JTS Benchmarking

To run build the JMH benchmarks:

    mvn -pl geomesa-spark/geomesa-spark-jts install
    mvn -pl geomesa-spark/geomesa-spark-jts-bench clean package
    
To run the packaged benchmarks:
    
    java -jar geomesa-spark/geomesa-spark-jts-bench/target/jmh-benchmarks.jar -rff json 
    
The output will be written to `jmh-results.json`

Additional commandline options are available when running the benchmarks, useful for overriding 
annotation defaults:

    java -jar geomesa-spark/geomesa-spark-jts-bench/target/jmh-benchmarks.jar -h
    

Note: http://jmh.morethan.io/ is a nice online tool for visualizing JSON-formatted JMH results, 
especially when comparing runs.
