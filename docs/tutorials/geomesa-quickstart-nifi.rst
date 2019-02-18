GeoMesa NiFi Quick Start
========================

This tutorial provides an example implementation for using GeoMesa with
NiFi. This walk-through will guide you in setting up the components
required for ingesting GDELT files into GeoMesa running on Accumulo.

Prerequisites
-------------

Before you begin, you must have the following:

-  an instance of Accumulo 1.7 or 1.8 running on Hadoop 2.2 or better
-  an Accumulo user that has both create-table and write permissions
-  the GeoMesa Accumulo distributed runtime `installed for your Accumulo
   instance <http://www.geomesa.org/documentation/user/installation_and_configuration.html#installing-the-accumulo-distributed-runtime-library>`__
-  a local copy of the `Java <http://java.oracle.com/>`__ JDK 8
-  Apache `Maven <http://maven.apache.org/>`__ installed
-  an instance of Apache `NiFi <http://nifi.apache.org/>`__ 0.4.1 or
   better
-  (Optional) an installation of `GeoServer <http://geoserver.org/>`__
   with the `GeoMesa Accumulo GeoServer
   plugin <http://www.geomesa.org/documentation/user/accumulo/install.html#install-accumulo-geoserver>`__
   to visualize the ingested data.
-  a GitHub client installed

About this Tutorial
-------------------

This quick start operates by reading CSV files from the local filesystem, and writing them to Accumulo
using the PutGeoMesaAccumulo processor.

Obtain GDELT data
-----------------

In this tutorial we will be ingesting GDELT data. If you already have some GDELT data downloaded, then
you may skip this section.

The `GDELT Event database <http://www.gdeltproject.org>`__ provides a comprehensive time- and location-indexed
archive of events reported in broadcast, print, and web news media worldwide from 1979 to today. You
can download raw GDELT data files at http://data.gdeltproject.org/events/index.html.

GeoMesa ships with the ability to parse GDELT data, and a script for downloading it. For more details,
see :ref:`gdelt_converter`.

.. note::

    GDELT is available in two different formats, the original and version 2. GeoMesa provides converters
    for both formats, but take note which format you download, as the converter name will differ.

Download and Build the GeoMesa NiFi project
-------------------------------------------

Pick a reasonable directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-nifi.git
    $ cd geomesa-nifi

To build, run

.. code-block:: bash

    $ mvn clean install

This will build several processors:

- ``PutGeoMesaAccumulo`` Ingest data into a GeoMesa Accumulo store
- ``PutGeoMesaHBase`` Ingest data into a GeoMesa HBase store
- ``PutGeoMesaKafka`` Ingest data into a GeoMesa Kafka store
- ``PutGeoMesaRedis`` Ingest data into a GeoMesa Redis store
- ``PutGeoMesaFileSystem`` Ingest data into a GeoMesa FileSystem store
- ``PutGeoTools`` Ingest data into an arbitrary GeoTools store
- ``ConvertToGeoAvro`` Convert data into Avro data files

This tutorial will use Accumulo, but any processor can be used with slight differences in the configuration.

Install the GeoMesa Processor
-----------------------------

Install the GeoMesa NiFi Processor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to use NiFi with GeoMesa we need to first install the GeoMesa
processor. To do this simply copy the ``geomesa-nifi-nar-$VERSION.nar``
that you just built from ``geomesa-nifi/geomesa-nifi-nar/target`` to the
``lib/`` directory of you NiFi installation.

Install the SFTs and Converters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Additionally we need to copy the ``geomesa-tools_2.11-$VERSION-data.jar``
from the geomesa binary distribution to the ``lib/`` of the NiFi installation.
The ``geomesa-tools_2.11-$VERSION-data.jar`` is located in the
``dist/converters`` directory of the geomesa Accumulo binary distribution. This
jar contains the SimpleFeatureType and converter definitions needed for GeoMesa to ingest the
GDELT data. You can obtain the binary distribution from `GitHub <https://github.com/locationtech/geomesa/releases>`__,
or you may build it locally from source.

Create a NiFi Flow
------------------

If you are not familiar with NiFi, follow the `Getting Started <https://nifi.apache.org/docs/nifi-docs/html/getting-started.html>`__
guide to familiarize yourself. The rest of this tutorial assumes a basic understanding of NiFi.

Create the GeoMesa processor by dragging a new processor to your flow, and selecting 'PutGeoMesaAccumulo'.
Select the processor and click the 'configure' button to configure it. On the properties page, fill out the following
values:

* **Mode**: ``Converter``
* **SftName**: ``gdelt`` or ``gdelt2`` (depending on what version of data you downloaded)
* **ConverterName**: ``gdelt`` or ``gdelt2`` (depending on what version of data you downloaded)
* **FeatureNameOverride**: ``gdelt``
* **accumulo.instance.id**: the ID of your Accumulo instance
* **accumulo.zookeepers**: the zookeeper hosts of your Accumulo instance
* **accumulo.user**: an Accumulo user with create and write permissions
* **accumulo.password**: the password for your Accumulo user
* **accumulo.catalog**: the catalog table you want to ingest data into

.. warning::

    If you have set up the GeoMesa Accumulo distributed runtime to be isolated within a namespace, as
    described in :ref:`install_accumulo_runtime`, the value of **accumulo.catalog** should include the
    namespace (e.g. ``myNamespace.gdelt``).

Now create a 'GetFile' processor, and hook it up as the input to the GeoMesa processor.

Once both processors are configured, you can start them through the NiFi UI.

Ingest the Data
---------------

Assuming you have configured your NiFi flow with a 'GetFile' processor, you can ingest data by copying the GDELT
data you downloaded into the processor's configured input path. Note that you will need to use plain files - if
the files are zipped, unzip them before ingesting.

You should see the data pass through the NiFi flow and be ingested.

Visualize the Data
------------------

Once the data has been ingested, you can use GeoServer to visualize it on a map. Follow the instructions
in the Accumulo quick-start tutorial, :ref:`accumulo_quickstart_visualize`.
