GeoMesa NiFi Quick Start
========================

This tutorial provides an example implementation for using GeoMesa with
NiFi. This walk-through will guide you in setting up the components
required for ingesting GDELT files into GeoMesa running on Accumulo.

Prerequisites
-------------

Before you begin, you must have the following:

-  an instance of Accumulo 1.9 or 2.0 running on Hadoop 2.8 or later
-  an Accumulo user that has both create-table and write permissions
-  the GeoMesa Accumulo distributed runtime :ref:`installed for your Accumulo instance <install_accumulo_runtime>`
-  a local copy of the `Java <http://java.oracle.com/>`__ JDK 8
-  Apache `Maven <http://maven.apache.org/>`__ installed
-  an instance of Apache `NiFi <http://nifi.apache.org/>`__ 1.11.4 or later
-  (Optional) an installation of `GeoServer <http://geoserver.org/>`__
   with the :ref:`GeoMesa Accumulo GeoServer plugin <install_accumulo_geoserver>`
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
archive of events reported in broadcast, print, and web news media worldwide from 1979 to today. The raw GDELT
version 1 data files are available to download at http://data.gdeltproject.org/events/index.html. The version 2
files can be downloaded at http://data.gdeltproject.org/gdeltv2/masterfilelist.txt.

GeoMesa ships with the ability to parse GDELT data, and a script for downloading it. For more details,
see :ref:`gdelt_converter`.

.. note::

    GDELT is available in two different formats, the original and version 2. GeoMesa provides converters
    for both formats, but take note which format you download, as the converter name will differ.

Download and Install the GeoMesa NiFi project
---------------------------------------------

Follow the instructions at :ref:`nifi_bundle` to download and install the appropriate NARs in your NiFi instance.

This tutorial will use Accumulo, but any processor can be used with slight differences in the configuration.

Create a NiFi Flow
------------------

If you are not familiar with NiFi, follow the `Getting Started <https://nifi.apache.org/docs/nifi-docs/html/getting-started.html>`__
guide to familiarize yourself. The rest of this tutorial assumes a basic understanding of NiFi.

Create the GeoMesa processor by dragging a new processor to your flow, and selecting 'PutGeoMesaAccumulo'.
Select the processor and click the 'configure' button to configure it. On the properties page, fill out the following
values:

* **SftName**: ``gdelt`` or ``gdelt2`` (depending on what version of data you downloaded)
* **FeatureNameOverride**: ``gdelt``
* **ConverterName**: ``gdelt`` or ``gdelt2`` (depending on what version of data you downloaded)
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
