# GeoMesa HBase Tools

## Introduction
GeoMesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Installation
To begin using the command line tools, first build the full GeoMesa project from the GeoMesa source directory with 

    mvn clean install

You can also make the build process significantly faster by adding `-DskipTests`. This will create a file called
``geomesa-hbase-{version}-bin.tar.gz`` in the ``geomesa-hbase/geomesa-hbase-dist/target`` directory. Untar this file with

    tar xvfz geomesa-hbase/geomesa-hbase-dist/target/geomesa-hbase-{version}-bin.tar.gz
    
Next, `cd` into the newly created directory with
    
    cd geomesa-tools-${version}

GeoMesa Tools relies on a GEOMESA_HBASE_HOME environment variable. Running
    
    source bin/geomesa-hbase configure

with the `source` shell function will set this for you, add GEOMESA_HBASE_HOME/bin to your PATH, and set your new
environment variables in your current shell session. Now you should be able to use GeoMesa from any directory on your computer.

Note: The tools will read the HBASE_HOME and HADOOP_HOME environment variables to load the appropriate JAR files
for HBase and Hadoop. In particular, you will need `hbase-site.xml` either implicitly from HBASE_HOME, or installed
directly in the tools classpath. Use the ``geomesa-hbase classpath`` command in order to see what JARs are being used. 

To test, `cd` to a different directory and run:

    geomesa-hbase

This should print out usage text

### Installing SFT and Converter Definitions

GeoMesa HBase Tools ships with embedded SimpleFeatureType and GeoMesa Converter definitions for common data
types including Twitter, GeoNames, T-drive, and many more. Users can add additional types by providing
a `reference.conf` file embedded with a jar within the `lib` directory or by registering
the `reference.conf` file in the `$GEOMESA_HBASE_HOME/conf/sfts` directory. 

For example, to add a type named `customtype`, create a directory named `$GEOMESA_HBASE_HOME/conf/sfts/customtype`
and then add the SFT and Conveter typesafe config to the a file named `$GEOMESA_HBASE_HOME/conf/sfts/customtype/reference.conf`.
This file will be automatically picked up and placed on the classpath when the tools are run.

### Downloading Common Datasets

GeoMesa ships with a script that will download common datasets and place them in a
`$GEOMESA_HBASE_HOME/data/<datatype>` directory. For example, to download gdelt data run:

    > $GEOMESA_HBASE_HOME/bin/download-data.sh gdelt
    Enter a date in the form YYYYMMDD: 20150101
    Saving to: “/tmp/geomesa-tools-1.2.3-SNAPSHOT/data/gdelt/20150101.export.CSV.zip”
    
    > unzip -P data/gdelt/ data/gdelt/20150101.export.CSV.zip 
    Archive:  data/gdelt/20150101.export.CSV.zip
      inflating: 20150101.export.CSV     

    > geomesa-hbase ingest -c catalog -s gdelt -C gdelt $GEOMESA_HBASE_HOME/data/gdelt/20150101.export.CSV

###Enabling Shape File Support
Due to licensing restrictions, a necessary dependency (jai-core) for shape file support must be manually installed:
    
    <dependency>
      <groupId>javax.media</groupId>
      <artifactId>jai_core</artifactId>
      <version>1.1.3</version>
    </dependency>
    
This library can be downloaded from your local nexus repo or `http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib.zip`

To install, copy the jai_core.jar and jai_codec.jar into `$GEOMESA_HBASE_HOME/lib/`

Optionally there is a script bundled as `$GEOMESA_HBASE_HOME/bin/install-jai.sh` that will attempt to wget and install
the jai libraries.

###Logging configuration
GeoMesa tools comes bundled by default with an slf4j implementation that is installed to the $GEOMESA_HBASE_HOME/lib directory
 named `slf4j-log4j12-1.7.5.jar` If you already have an slf4j implementation installed on your Java Classpath you may
 see errors at runtime and will have to exclude one of the JARs. This can be done by simply renaming the bundled
 `slf4j-log4j12-1.7.5.jar` file to `slf4j-log4j12-1.7.5.jar.exclude`
 
Note that if no slf4j implementation is installed you will see this error:

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
