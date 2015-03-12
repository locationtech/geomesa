# GeoMesa Jobs

### Building Instructions

This project contains map/reduce and scalding jobs for maintaining GeoMesa.

If you wish to build this project separately, you can with maven:

```shell
geomesa> mvn clean install -pl geomesa-jobs
```

To facilitate running jobs, you may wish to build a shaded jar that contains all the required dependencies.
Ensure that the pom references the correct versions of hadoop, accumulo etc for your cluster, then build the
project using the 'assemble' profile

```shell
geomesa> mvn clean install -P assemble -pl geomesa-jobs
```

The following instructions assume you have built a shaded jar; if not you will need to use the 'libjars'
argument to ensure the correct jars are available on the distributed classpath.

#### Attribute Indexing

GeoMesa provides indexing on attributes to improve certain queries. You can indicate attributes that should
be indexed when you create your schema (simple feature type). If you decide later on that you would like to
index additional attributes, you can use the attribute indexing job. You only need to run this job once.

The job can be invoked through yarn as follows (jar version may vary slightly):

```shell
geomesa> yarn jar geomesa-jobs/target/geomesa-jobs-accumulo1.5-1.0.0.jar \
    com.twitter.scalding.Tool \
    org.locationtech.geomesa.jobs.index.AttributeIndexJob \
    --hdfs \
    --geomesa.accumulo.instance <instance> \
    --geomesa.accumulo.zookeepers <zookeepers> \
    --geomesa.accumulo.user <user> \
    --geomesa.accumulo.password <pwd> \
    --geomesa.feature.tables.catalog <catalog-table> \
    --geomesa.feature.name <feature> \
    --geomesa.index.coverage <full|join> \ # optional attribute
    --geomesa.index.attributes <attributes to index (space separated)>
```

(Note that if you did not build with the 'assemble' profile, you will also need to include an extensive
-libjars argument with all dependent jars)

#### Transitioning Indices

Between rc4 and rc5, incompatible schema changes were made. If you have data in the old format, but would like
to update to the new version, you may use the SortedIndexUpdateJob.

The job can be invoked through yarn as follows (jar version may vary slightly):

```shell
yarn jar geomesa-jobs/target/geomesa-jobs-accumulo1.5-1.0.0.jar \
    com.twitter.scalding.Tool \
    org.locationtech.geomesa.jobs.index.SortedIndexUpdateJob \
    --hdfs \
    --geomesa.accumulo.instance <instance> \
    --geomesa.accumulo.zookeepers <zookeepers> \
    --geomesa.accumulo.user <user> \
    --geomesa.accumulo.password <pwd> \
    --geomesa.feature.tables.catalog <catalog-table> \
    --geomesa.feature.name <feature>
```

(Note that if you did not build with the 'assemble' profile, you will also need to include an extensive
-libjars argument with all dependent jars)
