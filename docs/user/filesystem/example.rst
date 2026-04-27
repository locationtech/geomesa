FileSystem Data Store Example
=============================

In this simple example we will ingest a small CSV into a local filesystem data store partitioned by a daily,z2-2bit
scheme. To begin, start by untaring the geomesa-fs distribution. Inside this distribution you will find an examples
folder which contains an example csv file that we will ingest. First download and extract the binary distribution:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-{{release}}/geomesa-fs_{{scala_binary_version}}-{{release}}-bin.tar.gz"
    $ tar xvf geomesa-fs_{{scala_binary_version}}-{{release}}-bin.tar.gz
    $ cd geomesa-fs_{{scala_binary_version}}-{{release}}
    $ cat examples/csv/example.csv

The output should look like::

    ID,Name,Age,LastSeen,Friends,Lon,Lat,Vis
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan","patronus->10,expelliarmus->9",-100.236523,23,user
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry","accio->10",40.232,-53.2356,user
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort","potions->10",3,-62.23,user&admin

As you can see, there are 3 records in the file. GeoMesa ships with a pre-installed SimpleFeatureType and converter
for this example file which can be found in the ``conf/application.conf`` file. Running ``bin/geomesa-fs env`` will
show that there is an ``example-csv`` type and converter installed along with many other types including gdelt, ais, etc:

.. code-block:: bash

    $ bin/geomesa-fs env

The output should look like::

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

    $ bin/geomesa-fs ingest -p /tmp/dstest --metadata-type file -s example-csv -C example-csv \
      --partition-scheme daily,z2:bits=2 examples/csv/example.csv

The output should look like::

    INFO  Creating schema example-csv
    INFO  Running ingestion in local mode
    INFO  Ingesting 1 file with 1 thread
    [============================================================] 100% complete 3 ingested 0 failed in 00:00:01
    INFO  Local ingestion complete in 00:00:01
    INFO  Ingested 3 features with no failures.

We can verify our ingest by running an export:

.. code-block:: bash

    $ bin/geomesa-fs export -p /tmp/dstest -f example-csv --metadata-type file

The output should look like::

    id,fid:Integer:index=false,name:String:index=true,age:Integer:index=false,lastseen:Date:default=true:index=false,*geom:Point:srid=4326
    26236,26236,Hermione,25,2015-06-07T00:00:00.000Z,POINT (40.232 -53.2356)
    3233,3233,Severus,30,2015-10-23T00:00:00.000Z,POINT (3 -62.23)
    23623,23623,Harry,20,2015-05-06T00:00:00.000Z,POINT (-100.236523 23)
    INFO  Feature export complete to standard out in 1676ms for 3 features

Now lets inspect the filesystem:

.. code-block:: bash

    $ find /tmp/dstest | sort

The output should look something like::

    /tmp/dstest
    /tmp/dstest/1010
    /tmp/dstest/1010/0111
    /tmp/dstest/1010/0111/0111
    /tmp/dstest/1010/0111/0111/00100000
    /tmp/dstest/1010/0111/0111/00100000/w_example-csv_65929c1f102c4014bf2302924304339f.parquet
    /tmp/dstest/1010/1000
    /tmp/dstest/1010/1000/0011
    /tmp/dstest/1010/1000/0011/00111110
    /tmp/dstest/1010/1000/0011/00111110/w_example-csv_4d8630199c614e189d0e25af9bc21e53.parquet
    /tmp/dstest/1011
    /tmp/dstest/1011/1000
    /tmp/dstest/1011/1000/0101
    /tmp/dstest/1011/1000/0101/00010011
    /tmp/dstest/1011/1000/0101/00010011/w_example-csv_3cf140998e08439397ecb53425dcb2d1.parquet
    /tmp/dstest/metadata
    /tmp/dstest/metadata/.example_2dcsv_files.json
    /tmp/dstest/metadata/example_2dcsv.json

Note that the file paths are laid out based on a hash, in order to optimize read/write throughput. The data files include
the feature type name::

    /tmp/dstest/1011/1000/0101/00010011/w_example-csv_3cf140998e08439397ecb53425dcb2d1.parquet

Because we specified ``--metadata-type file``, the metadata for each data file is stored as json (the other option is to store
metadata in a relational database). This includes the partitions and various bounds for each file::

    $ cat /tmp/dstest/metadata/.example_2dcsv_files.json | jq . | head -n 30

::

    [
      {
        "file": "1010/1000/0011/00111110/w_example-csv_4d8630199c614e189d0e25af9bc21e53.parquet",
        "partition": [
          {
            "name": "days:attribute=lastseen",
            "value": "800040b1"
          },
          {
            "name": "z2:attribute=geom:bits=2",
            "value": "2"
          }
        ],
        "count": 1,
        "action": "Append",
        "spatialBounds": [
          {
            "attribute": 4,
            "xmin": -100.2365,
            "ymin": 23.0,
            "xmax": -100.2365,
            "ymax": 23.0
          }
        ],
        "attributeBounds": [
          {
            "attribute": 3,
            "lower": "8000014d26859c00",
            "upper": "8000014d26859c00"
          }
