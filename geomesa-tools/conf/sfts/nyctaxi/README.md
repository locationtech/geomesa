# NYC Taxi 

New York City taxi activity data published by University of Illiois from Freedom of Information Law requests to NYC Taxi and Limo Commission. More information about the dataset is [here](https://publish.illinois.edu/dbwork/open-data/). 

U of Illinois hosts the data in a Box web interface and arrives in several zip archives. Only the "trip data" is examined here. There is also "fare data" from the taxi meters such as fare amount and tip. Once unpacked there are monthly CSV files covering four years.

## Getting the Data

The .zip files are available for download [here](https://databank.illinois.edu/datasets/IDB-9610843). Select the desired zip files unzip them into a convenient directory.

The taxi data includes two points and two timestamps per record in the original data. This presents an opportunity for different simple feature type designs. 

There is one design where each SFT is either a pickup or dropoff point with timestamp. The two points share a common trip ID. Internally they are differentiated by hashing the record with either "pickup" or "dropoff" appended. The advantage of this for geomesa demonstrations is dealing with a larger quantity of point data.

In keeping with the original data, there is a two-point feature type as well. 

One could also imagine a linetype geometry, however the path is underspecified. Previous analysis of the NYC Taxi data seems to have used Google Maps API calls to propose valid paths through the streets. We do not attempt that in this work.

## Sample ingest command

Check that the `nyctaxi` and `nyctaxi-single` simple feature types are available on the GeoMesa tools classpath. This is the default case.

    geomesa env | grep 'nyctaxi\|nyctaxi-single'

If they are not, merge the contents of `reference.conf` with `$GEOMESA_HOME/conf/application.conf`, or ensure that `reference.conf` is in `$GEOMESA_HOME/conf/sfts/nyctaxi` 

### Two record method

To ingest with GeoMesa command line interface, first the pickup:

    $ geomesa ingest -u username -c catalogName -s nyctaxi -C nyctaxi hdfs://namenode:9000/path/to/nyctaxi*

Then the dropoff ingests the same file with the same simple feature type, but different converter.

    $ geomesa ingest -u username -c catalogName -s nyctaxi -C nyctaxi-drop hdfs://namenode:9000/path/to/nyctaxi*

### Single record method

    $ geomesa ingest -u username -c catalogName -s nyctaxi-single -C nyctaxi-single.conf hdfs://namenode:9000/path/to/nyctaxi*
