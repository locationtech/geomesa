FileSystem Datastore Example
============================

In this simple example we will ingest a small CSV into a local filesystem datastore partitioned by an daily,z2-2bit
scheme. To begin, start by untaring the geomesa-fs distribution. Inside this distribution you will find an examples
folder which contains an example csv file that we will ingest:

.. code-block:: bash

    $ cd /tmp/
    $ tar xvf ~/Downloads/geomesa-fs-dist_2.11-$VERSION.tar.gz
    $ cd /tmp/geomesa-fs_2.11-$VERSION

    $ cat examples/ingest/csv/example.csv
    ID,Name,Age,LastSeen,Friends,Lon,Lat,Vis
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan","patronus->10,expelliarmus->9",-100.236523,23,user
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry","accio->10",40.232,-53.2356,user
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort","potions->10",3,-62.23,user&admin

As you can see, there are 3 records in the file. GeoMesa ships with a pre-installed SimpleFeatureType and converter
for this example file which can be found in the ``conf/application.conf`` file. Running ``bin/geomesa-fs env`` will
show that there is an ``example-csv`` type and converter installed along with many other types including twitter, gdelt,
osm, etc:

.. code-block:: bash

    $ bin/geomesa-fs env
        Simple Feature Types:
        example-avro
        example-csv
        example-csv-complex
        ...

        Simple Feature Type Converters:
        example-avro-header
        example-avro-no-header
        example-csv
        ...

For this example we'll ingest the three rows to a local filesystem. Note that the records are all on different days::

    2015-05-06
    2015-06-07
    2015-10-23

and that the geometries fall into two quadrants of the world. The first record is in the upper left quadrant and the
second and third records are in the lower right quadrant::

    Lon          Lat
    -100.236523  23
    40.232      -53.2356
    3           -62.23

Now lets ingest.

.. code-block:: bash

    $ bin/geomesa-fs ingest -p file:///tmp/dstest -e parquet -s example-csv -C example-csv \
    --partition-scheme daily,z2-2bit examples/ingest/csv/example.csv

    INFO  Creating schema example-csv
    INFO  Running ingestion in local mode
    INFO  Ingesting 1 file with 1 thread
    [============================================================] 100% complete 3 ingested 0 failed in 00:00:01
    INFO  Local ingestion complete in 00:00:01
    INFO  Ingested 3 features with no failures.

We can verify our ingest by running an export:

.. code-block:: bash

    $ bin/geomesa-fs export -p file:///tmp/dstest -f example-csv

    id,fid:Integer:index=false,name:String:index=true,age:Integer:index=false,lastseen:Date:default=true:index=false,*geom:Point:srid=4326
    26236,26236,Hermione,25,2015-06-07T00:00:00.000Z,POINT (40.232 -53.2356)
    3233,3233,Severus,30,2015-10-23T00:00:00.000Z,POINT (3 -62.23)
    23623,23623,Harry,20,2015-05-06T00:00:00.000Z,POINT (-100.236523 23)
    INFO  Feature export complete to standard out in 1676ms for 3 features

Now lets inpsect the filesystem to see what it looks like:

.. code-block:: bash

    $ find /tmp/dstest
    /tmp/dstest
    /tmp/dstest/example-csv
    /tmp/dstest/example-csv/2015
    /tmp/dstest/example-csv/2015/05
    /tmp/dstest/example-csv/2015/05/06
    /tmp/dstest/example-csv/2015/05/06/2
    /tmp/dstest/example-csv/2015/05/06/2/.0000.parquet.crc
    /tmp/dstest/example-csv/2015/05/06/2/0000.parquet
    /tmp/dstest/example-csv/2015/10
    /tmp/dstest/example-csv/2015/10/23
    /tmp/dstest/example-csv/2015/10/23/1
    /tmp/dstest/example-csv/2015/10/23/1/.0000.parquet.crc
    /tmp/dstest/example-csv/2015/10/23/1/0000.parquet
    /tmp/dstest/example-csv/2015/06
    /tmp/dstest/example-csv/2015/06/07
    /tmp/dstest/example-csv/2015/06/07/1
    /tmp/dstest/example-csv/2015/06/07/1/.0000.parquet.crc
    /tmp/dstest/example-csv/2015/06/07/1/0000.parquet
    /tmp/dstest/example-csv/schema.sft
    /tmp/dstest/example-csv/.metadata.crc
    /tmp/dstest/example-csv/.schema.sft.crc
    /tmp/dstest/example-csv/metadata


Notice that we have a directory structure laid out based on our ``daily,z2-2bit`` scheme. Notice the first parquet
file path is composed of a date path ``2016/05/06`` and then a z2 ordinate of ``2`` ::

    /tmp/dstest/example-csv/2015/05/06/2/0000.parquet

The parquet file name is ``0000`` which indicates it is the first file we have ingested in this spatio-temporal
filesystem partition. If we were to ingest a second file it would be named ``0001.parquet`` and GeoMesa would read the
contents of both at query time.

We'll also take a quick look at the metadata to see that it lists the parquet files in the system:

.. code-block:: bash

    $ cat /tmp/dstest/example-csv/metadata
    {
        "partitions" : {
            "2015/05/06/2" : [
                "0000.parquet"
            ],
            "2015/06/07/1" : [
                "0000.parquet"
            ],
            "2015/10/23/1" : [
                "0000.parquet"
            ]
        }
    }

