Getting Started
===============

This chapter highlights several features of GeoMesa, along with tutorials for getting started.

Quick Start
-----------

The GeoMesa quick start tutorials are the fastest and easiest way to get started with GeoMesa.
They are a good stepping-stone on the path to the other tutorials that present increasingly involved examples
of how to use GeoMesa. The tutorials show how to write custom Java code to ingest and query data with GeoMesa,
and visualize the changes being made in GeoServer.

Quick starts are available for several back-end databases:

* :doc:`/tutorials/geomesa-quickstart-accumulo`
* :doc:`/tutorials/geomesa-quickstart-hbase`
* :doc:`/tutorials/geomesa-quickstart-cassandra`

GeoDocker: Bootstrapping GeoMesa Accumulo and Spark on AWS
----------------------------------------------------------

Getting started with spatio-temporal analysis with GeoMesa, Accumulo, and Spark on Amazon Web Services (AWS)
is incredibly simple, thanks to the `Geodocker <https://github.com/geodocker/geodocker-geomesa>`_ project.
The guide below describes how to bootstrap a GeoMesa Accumulo cluster using Amazon ElasticMapReduce (EMR) and
Docker in order to ingest and query sample GDELT data.

See :doc:`/tutorials/geodocker-geomesa/geodocker-geomesa-spark-on-aws`.

GeoMesa Kafka
-------------

The GeoMesa Kakfa Quick Start tutorial shows how to write custom Java code to produce and consume messages in
Apache Kafka using GeoMesa, query the data and visualize the changes being made in Kafka with GeoServer.

See :doc:`/tutorials/geomesa-quickstart-kafka`.

Storm Analysis
--------------

GeoMesa can leverage the `Apache Storm`_ distributed computation system to ingest and analyze
geospatial data in near real time. The :doc:`/tutorials/geomesa-quickstart-storm` tutorial
shows how to use Kafka, GeoMesa, and Storm to parse Open Street Map data files and ingest
them into Accumulo.

See :doc:`/tutorials/geomesa-quickstart-storm`.

.. _Apache Storm: http://storm.apache.org/

GeoJSON
-------

GeoMesa provides built-in integration with GeoJSON. GeoMesa provides a GeoJSON API
that allows for the indexing and querying of GeoJSON data without using the GeoTools
API--all data and operations are pure JSON. The API also includes a REST endpoint for
web integration.

See :doc:`/user/geojson`.