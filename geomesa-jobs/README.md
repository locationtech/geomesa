# GeoMesa Jobs

### Building Instructions

If you wish to build this project separately, you can with mvn clean install

```geomesa/geomesa-jobs> mvn clean install```

This project contains map/reduce and scalding jobs for maintaining GeoMesa.

#### Attribute Indexing

To index GeoMesa attributes on demand, you can call:

```scala
AttributeIndexJob.runJob(conf: Configuration, params: Map[String, String], feature: String, attributes: Seq[String])
```

You can also run through Hadoop Yarn on the command line:

```shell
yarn jar target/geomesa-jobs-accumulo1.5-1.0.0-SNAPSHOT.jar \
    com.twitter.scalding.Tool \
    org.locationtech.geomesa.jobs.index.AttributeIndexJob \
    --hdfs \
    --geomesa.accumulo.instance <instance> \
    --geomesa.accumulo.zookeepers <zookeepers> \
    --geomesa.accumulo.user <user> \
    --geomesa.accumulo.password <pwd> \
    --geomesa.feature.tables.catalog <catalog-table> \
    --geomesa.feature.tables.record <record-table> \
    --geomesa.feature.tables.attribute <attribute-index-table> \
    --geomesa.feature.name <feature> \
    --geomesa.index.attributes <attributes to index (space separated)>
```

(Note that this command also requires an extensive -libjars argument with all dependent jars)
