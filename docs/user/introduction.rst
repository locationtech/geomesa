Introduction
============

What is GeoMesa?
----------------

GeoMesa is an open-source, distributed, spatio-temporal database and suite of tools built on top
of the Apache Accumulo (http://accumulo.apache.org/) column family store. It lets users quickly store, index, and query both stored and streaming geospatial data at scale. 

GeoMesa is specifically built to manage large spatio-temporal datasets such as tracks, social media, IoT data, and other location data from major mobile applications. Through standards-based interfaces, it can drive map user interfaces and also serve up data for analytics such as queries, histograms, heat maps, and time series analysis.

GeoMesa features include the ability to:

* Store gigabytes to petabytes of spatial data (tens of billions of points or more)
* Serve up tens of millions of points in seconds
* Ingest data faster than 10,000 records per second per node
* Scale horizontally easily (add more servers to add more capacity)
* Support Spark analytics
* Drive a map through GeoServer or other OGC Clients

In addition to Accumulo, GeoMesa can also use Apache HBase and Google Cloud Bigtable for storage.

.. according to https://en.wikipedia.org/wiki/GeoMesa

There are many reasons that GeoMesa can provide the best solution to your spatio-temporal database needs:

* You have Big Spatial Data sets and you are reaching performance limitations of relational database systems. Perhaps you are looking at sharding strategies and wondering if now is the time to look for a new storage solution.
* You have very high velocity data and need high read and write speeds.
* Your analytics operate in the cloud—perhaps your analytics run in Spark and you want to enable spatial analytics.
* You are looking for a supported open-source alternative to expensive proprietary solutions.
* You are looking for a Platform as a Service (PaaS) Database where you can store Big Spatial Data.
* Filter data using the rich Common Query Language defined by the OGC.

Community and Support
---------------------

The GeoMesa website may be found at http://www.geomesa.org/. For additional information, see:

* The tutorials on the main GeoMesa website: http://www.geomesa.org/tutorials/
* The GeoMesa FAQ: http://www.geomesa.org/faq/
* The GeoMesa Users (https://locationtech.org/mhonarc/lists/geomesa-users/) and 
  Dev (https://locationtech.org/mhonarc/lists/geomesa-dev/) mailing list archives
* ``README.md`` files provided under most modules
* The chat at https://gitter.im/locationtech/geomesa/.

|locationtech-icon|

GeoMesa is a member of the `LocationTech <http://www.locationtech.org>`_ working group of the Eclipse Foundation.

License
-------

GeoMesa is open-source software, and is licensed under the Apache License Version 2.0: 
http://apache.org/licenses/LICENSE-2.0.html

.. |locationtech-icon| image:: _static/img/locationtech.png

