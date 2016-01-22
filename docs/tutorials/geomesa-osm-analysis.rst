Streaming Ingest
================

This tutorial shows you how to:

1. Quickly and easily ingest big OSM (Open Street Map) data files into a
   GeoMesa Accumulo table via a Kafka/Storm stream.
2. Leverage GeoServer to query and visualize the data.

Prerequisites
-------------

.. warning::

    You will need access to a Kafka 0.8 installation, a Storm 0.8 installation as well as an Accumulo |accumulo_version| database.

You should have access to:

-  an instance of Accumulo |accumulo_version| running on Hadoop
   2.2,
-  an Accumulo user with create-table and write permissions,
-  an installation of Kafka 0.8,
-  an installation of Storm 0.8, and
-  an instance of GeoServer 2.5.2 with the GeoMesa plugin installed

Several tools should also be installed and configured:

-  The `xz <http://tukanni.org/xz/>`__ data compression tool,
-  Java JDK 7,
-  `Apache Maven <http://maven.apache.org>`__ 3.2.2 or better, and
-  a `git <http://git-scm.com/>`__ client.

Ingest
------

Obtaining OSM data
~~~~~~~~~~~~~~~~~~

Download the `Open Street Map <http://planet.openstreetmap.org/>`__
(OSM) data file `simple-gps-points-120312.txt` from `http://planet.openstreetmap.org/gps/ <http://planet.openstreetmap.org/gps/>`__.
This might take a while; the file is approximately 7 GB.

Use the following command to unpack the data, adjusting the filename here and in subsequent commands if you downloaded a more recent version:

.. code-block:: bash

    $ xz simple-gps-points-120312.txt.xz

Note: In this demonstration, we will use the ``simple-gps-points`` OSM
data that contains only the location of an observation.

Building the example code
~~~~~~~~~~~~~~~~~~~~~~~~~

Clone the ``geomesa`` and ``geomesa-tutorials`` projects:

.. code-block:: bash

    $ git clone https://github.com/locationtech/geomesa.git
    $ git clone https://github.com/geomesa/geomesa-tutorials.git

Build GeoMesa, if you haven't already:

.. code-block:: bash

    $ cd geomesa
    $ mvn clean install

The source code for this tutorial is in the ``geomesa-quickstart-storm``
directory of ``geomesa-tutorials``:

.. code-block:: bash

    $ cd ../geomesa-tutorials/geomesa-quickstart-storm
    $ mvn clean install

DataStore initialization
~~~~~~~~~~~~~~~~~~~~~~~~

`GeoTools <http://geotools.org/>`__ uses a ``SimpleFeatureType`` to
represent the schema for a feature source. We can easily create a schema
for the OSM feature type using the DataUtilities class. The schema
string is a comma separated list of attributes in the form
``<ATTRIBUTE_NAME>:<ATTRIBUTE_CLASSNAME>:<ATTRIBUTE_HINT>``, where the
hint is optional (for example,
"geom:Point:srid=4326,name:String,year:Integer"). In the tutorial code
in ``geomesa-osm``, our ``SimpleFeatureType`` only needs to represent
the geometry, a single ``Point``.

.. code-block:: java

    SimpleFeatureType featureType = DataUtilities.createType(featureName, "geom:Point:srid=4326");

We create the new feature type in GeoMesa as follows.

.. code-block:: java

    ds.createSchema(featureType);

Setting Up the Ingest Topology
------------------------------

Use ``storm jar`` to submit the topology built
(``geomesa-osm/target/geomesa-quickstart-storm-$VERSION.jar``) to your Storm
Nimbus.

.. code-block:: bash

    $ storm jar geomesa-quickstart-storm-$VERSION.jar \
       com.example.geomesa.storm.OSMIngest                         \
       -instanceId <accumulo-instance-id>            \
       -zookeepers <zookeeper-hosts-string>          \
       -user <username> -password <password>         \
       -auths <comma-separated-authorization-string> \
       -tableName OSM -featureName event             \
       -topic OSM                                    \

Note that authorizations are optional. Unless you know that your table
already exists with explicit authorizations, or that it will be created
with default authorizations, you probably want to omit this parameter.

Setting Up the Kafka Topic
--------------------------

Now we are going to create a Kafka topic. Kafka serves as the entry
point into our Storm topology. We create a topic with several partitions
to parallelize the ingest both from the producer side as well as from
the consumer side.

.. code-block:: bash

    $ kafka-create-topic.sh       \
       --zookeeper <zookeepers> \
       --replica 3              \
       --partition 10           \
       --topic OSM              \

Create a Kafka producer to convert the ingest file into kafka messages.

.. code-block:: bash

    $ java -cp geomesa-quickstart-storm-$VERSION.jar     \
       com.example.geomesa.storm.OSMIngestProducer   \
       -ingestFile simple-gps-points-120312.txt      \
       -topic OSM                      \
       -brokers <kafka broker list>    \

Note that Kafka's default partitioner class assigns a message partition
based on a hash of the provided key. If no key is provided, all messages
are assigned the same partition.

.. code-block:: java
    :linenos:

    for (String x = bufferedReader.readLine(); x != null; x = bufferedReader.readLine()) {
        producer.send(new KeyedMessage<String, String>(topic, String.valueOf(rnd.nextInt()), x));
    }

Storm Spouts and Bolts
----------------------

In our example, the Storm ``Spout``\ s will consume messages from a
Kafka topic and send them through the ingest topology.

.. code-block:: java
    :linenos:

    public void nextTuple() {
        if(kafkaIterator.hasNext()) {
            List<Object> messages = new ArrayList<Object>();
            messages.add(kafkaIterator.next().message());
            _collector.emit(messages);
        }
    }

In our example, the ``Bolt``\ s parse the message, create and write
``Feature``\ s. In the ``prepare`` method of the ``Bolt`` class, we grab
the connection params that were initialized in the constructor and get a
handle on a ``FeatureWriter``.

.. code-block:: java
    :linenos:

    ds = DataStoreFinder.getDataStore(connectionParams);
    SimpleFeatureType featureType = ds.getSchema(featureName);
    featureBuilder = new SimpleFeatureBuilder(featureType);
    featureWriter = ds.getFeatureWriter(featureName, Transaction.AUTO_COMMIT);

The input to the ``Bolt``'s execute method is a ``Tuple`` containing a
``String``. We split the ``String`` on '%' to get individual points. For
each point, we split on commas to extract the attributes. We parse the
latitude and longitude field to set the default geometry of our
``SimpleFeature``. Note that OSM latitude and longitude values are
stored as integers that must be divided by 107.

.. code-block:: java
    :linenos:

    private Geometry getGeometry(final String[] attributes) {
        final Double lat = (double)Integer.parseInt(attributes[LATITUDE_COL_IDX]) / 1e7;
        final Double lon = (double)Integer.parseInt(attributes[LONGITUDE_COL_IDX]) / 1e7; 
        return geometryFactory.createPoint(new Coordinate(lon, lat));
    }
    
    public void execute(Tuple tuple) { 
        featureBuilder.reset(); 
        final SimpleFeature simpleFeature =
            featureBuilder.buildFeature(String.valueOf(UUID.randomUUID().getMostSignificantBits()));
        SimpleFeature.setDefaultGeometry(getGeometry(attributes));

        try {
            final SimpleFeature next = featureWriter.next();
            for (int i = 0; i < simpleFeature.getAttributeCount(); i++) {
                next.setAttribute(i, simpleFeature.getAttribute(i));
            }
            ((FeatureIdImpl)next.getIdentifier()).setID(simpleFeature.getID());
            featureWriter.write();
        }
    }


Analyze
-------

GeoServer Setup
~~~~~~~~~~~~~~~

First, make sure that GeoServer is installed and configured to use GeoMesa as described in the :doc:`../user/installation_and_configuration` section of the GeoMesa User Manual.

Register the GeoMesa DataStore with GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Click "Stores" and "Add new Store". If you do not see the Accumulo
Feature Data Store listed under Vector Data Sources, ensure the plugin
is in the right directory and restart GeoServer.

.. figure:: _static/img/tutorials/2014-04-17-geomesa-gdelt-analysis/Accumulo_Feature_Data_Store.png
   :alt: "Registering new Data Store"

   "Registering new Data Store"

Register the newly created Accumulo table using the same parameters
specified in the command line above. (If you use a workspace:layer name
other than geomesa:gdelt, you will need to change the WMS requests that
follow.)

.. figure:: _static/img/tutorials/2014-05-16-geomesa-osm-analysis/GeoserverAccumuloStoreRegistration.png
   :alt: "Registering new Accumulo Feature Data Store"

   "Registering new Accumulo Feature Data Store"

PUBLISH LAYER
~~~~~~~~~~~~~

After registering the DataStore, click to publish the layer. You will be
taken to the Edit Layer screen. In the Data pane, enter values for the
bounding boxes. For the whole world, use [-180,-90,180,90].

QUERY
~~~~~

Let's look at events in Chicago. The default point style is a red square
that does not suit our purposes. Add the SLD file 
:download:`OSMPoint.sld <_static/assets/tutorials/2014-05-16-geomesa-osm-analysis/OSMPoint.sld>`
to GeoServer.

.. code-block:: bash

    http://localhost:8080/geoserver/wms?service=WMS&version=1.1.0&request=GetMap&layers=geomesa:OSM&styles=OSMPoint&bbox=-87.63,41.88,-87.61,41.9&width=1400&height=600&srs=EPSG:4326&format=application/openlayers

.. figure:: _static/img/tutorials/2014-05-16-geomesa-osm-analysis/ChicagoPoint.png
   :alt: "Showing all OSM events in Chicago before Mar 12, 2012"

   "Showing all OSM events in Chicago before Mar 12, 2012"

HEATMAPS
~~~~~~~~

Use a heatmap to more clearly visualize multiple events in the same
location or high volume of data in general. Add the SLD file
:download:`heatmap.sld <_static/assets/tutorials/2014-04-17-geomesa-gdelt-analysis/heatmap.sld>`
to GeoServer.

.. code-block:: bash

    http://localhost:8080/geoserver/wms?service=WMS&version=1.1.0&request=GetMap&layers=geomesa:OSM&styles=heatmap&bbox=-87.63,41.88,-87.61,41.9&width=1400&height=600&srs=EPSG:4326&format=application/openlayers

.. figure:: _static/img/tutorials/2014-05-16-geomesa-osm-analysis/ChicagoDensity.png
   :alt: "Showing heatmap of OSM events in Chicago before Mar 12, 2012"

   "Showing heatmap of OSM events in Chicago before Mar 12, 2012"
