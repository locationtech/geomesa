Getting Started
===============

The first step to getting started with GeoMesa is to choose a persistent storage solution. This may be dictated
by your target environment, but if not there are several options available.

If you want a near real-time view of streaming data, then consider using
:doc:`Kafka </tutorials/geomesa-quickstart-kafka>` or :doc:`Redis </tutorials/geomesa-quickstart-redis>`.

Otherwise, you can get similar functionality through :doc:`HBase </tutorials/geomesa-quickstart-hbase>`,
:doc:`Accumulo </tutorials/geomesa-quickstart-accumulo>`, :doc:`Cassandra </tutorials/geomesa-quickstart-cassandra>`,
Google Bigtable or :doc:`Apache Kudu </tutorials/geomesa-quickstart-kudu>`. HBase and Accumulo support distributed
processing, so may be faster for certain operations. HBase and Cassandra are the most widely-used technologies,
while Accumulo is often chosen for its advanced security features.

Another option is the :doc:`FileSystem </tutorials/geomesa-quickstart-fsds>` data store, which has a very low
barrier to entry, and can read existing data in a variety of file formats. The FileSystem data store can provide
extremely low-cost storage when backed by cloud-native object stores; however, it generally is not as performant as
using an actual database.

For advanced use cases, multiple stores can be combined through a :doc:`/user/merged_view` to provide both high
performance (for recent data) and low cost (for older data).

Whichever storage solution you choose, the GeoMesa API is the same (outside of some back-end-specific configuration
options). For most users, the back-end can be swapped out with minimal code changes.

Quick Starts
------------

The GeoMesa :doc:`quick start tutorials </tutorials/index>` are the fastest and easiest way to get started with
GeoMesa. They are a good stepping-stone on the path to the other tutorials that present increasingly involved
examples of how to use GeoMesa. The tutorials show how to write custom Java code to ingest and query data with
GeoMesa, and visualize the changes being made in GeoServer.

Docker Images
-------------

The `Geodocker <https://github.com/geodocker/geodocker-geomesa>`_ project provides Docker images that make it easy
to stand up an Accumulo cluster with GeoMesa already configured. This
:doc:`guide </tutorials/geodocker-geomesa/geodocker-geomesa-spark-on-aws>` describes how to bootstrap a cluster
using Amazon ElasticMapReduce (EMR) and Docker in order to ingest and query sample GDELT data.

Data Ingestion
--------------

GeoMesa provides an :doc:`ingestion framework </user/convert/index>` that can be configured using JSON, which
means that your data can be ingested without writing any code. This makes it quick and easy to get started with
your custom data formats, and updates can be handled on-the-fly, without code changes.

GeoJSON
-------

GeoMesa provides built-in integration with GeoJSON. GeoMesa provides a :doc:`GeoJSON API </user/geojson>`
that allows for the indexing and querying of GeoJSON data without using the GeoTools
API -- all data and operations are pure JSON. The API also includes a REST endpoint for
web integration.

Spark
-----

GeoMesa provides spatial functionality on top of Spark and Spark SQL. To get started, see :ref:`tutorials_analytics`.
