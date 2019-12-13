Introduction
============

What is GeoMesa?
----------------

GeoMesa is an Apache-licensed, open-source suite of tools that enables large-scale geospatial analytics on
distributed computing systems, letting you manage and analyze the huge spatio-temporal datasets that IoT,
social media, tracking, and mobile phone applications seek to take advantage of today.

GeoMesa does this by providing spatio-temporal data persistence on top of popular distributed databases for
massive storage of point, line, and polygon data. It allows rapid access to this data via queries that take full
advantage of geographical properties to specify distance and area. GeoMesa also provides support for near real
time stream processing of spatio-temporal data by layering spatial semantics on top of the Apache Kafka messaging
system.

Through a geographical information server such as GeoServer, GeoMesa facilitates integration with a wide range of
existing mapping clients by enabling access to its databases and streaming capabilities over standard OGC (Open
Geospatial Consortium) APIs and protocols such as WFS and WMS. These interfaces also let GeoMesa drive map user
interfaces and serve up data for analytics such as queries, histograms, heat maps, and time series analyses.

GeoMesa features include the ability to:

* Store gigabytes to petabytes of spatial data (tens of billions of points or more)
* Serve up tens of millions of points in seconds
* Ingest data faster than 10,000 records per second per node
* Scale horizontally easily (add more servers to add more capacity)
* Support Spark analytics
* Drive a map through GeoServer or other OGC Clients

There are many reasons that GeoMesa can provide the best solution to your spatio-temporal database needs:

* You have Big Spatial Data sets and are reaching performance limitations of relational database systems. Perhaps
  you are looking at sharding strategies and wondering if now is the time to look for a new storage solution.
* You have very high-velocity data and need high read and write speeds.
* Your analytics operate in the cloud, perhaps using Spark, and you want to enable spatial analytics.
* You are looking for a supported, open-source alternative to expensive proprietary solutions.
* You are looking for a Platform as a Service (PaaS) database where you can store Big Spatial Data.
* You want to filter data using the rich Common Query Language (CQL) defined by the OGC.

Not sure where to begin? Take a look at :doc:`/user/getting_started`.

Community and Support
---------------------

The main GeoMesa website is http://www.geomesa.org/. For additional information, see:

* Getting started `tutorials <http://www.geomesa.org/tutorials/>`_
* The GeoMesa `users <https://locationtech.org/mhonarc/lists/geomesa-users/>`_ and
  `developers <https://locationtech.org/mhonarc/lists/geomesa-dev/>`_ mailing list archives
* The community chat on `Gitter <https://gitter.im/locationtech/geomesa/>`_.

|locationtech-icon|

GeoMesa is a member of the `LocationTech <http://www.locationtech.org/projects/technology.geomesa>`_ working group
of the Eclipse Foundation.

License
-------

GeoMesa is open-source software, and is licensed under the Apache License Version 2.0: 
http://apache.org/licenses/LICENSE-2.0.html

.. |locationtech-icon| image:: _static/img/locationtech.png

