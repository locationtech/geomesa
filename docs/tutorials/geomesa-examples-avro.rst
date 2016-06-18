GeoMesa Avro Binary Format Example
==================================

This example shows Java code for creating a ``SimpleFeatureCollection``,
serializing and deserializing to and from the Avro format.

Prerequisites
-------------

None.

Download and Build the Tutorial
-------------------------------

Pick a reasonable directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-tutorials.git
    $ cd geomesa-tutorials

To build, run

.. code-block:: bash

    $ mvn clean install -pl geomesa-examples-avro``

Run the Tutorial
----------------

On the command-line, run:

.. code-block:: bash

    $ java -cp geomesa-examples-avro/target/geomesa-examples-avro-$VERSION.jar com.example.geomesa.avro.AvroExample

The code will print out the 10 arbitrary SimpleFeatures which were
created and then serialized / deserialized. Example output follows.

::

     Creating 10 features.
     Writing features to Avro binary format.
     Reading features from Avro binary format.
     1.  Addams|0|Tue Sep 02 09:28:00 EDT 2014|POINT (-76.0577293170671 -37.615979973322965)|null
     2.  Bierce|1|Thu Apr 17 06:10:54 EDT 2014|POINT (-77.48469253216224 -38.87427143505418)|null
     3.  Clemens|2|Wed Jul 16 04:25:53 EDT 2014|POINT (-77.23040004571872 -37.8682338758089)|null
     4.  Addams|3|Fri Jan 03 02:06:31 EST 2014|POINT (-77.77384710048051 -37.79568311691846)|null
     5.  Bierce|4|Fri Jun 13 10:42:56 EDT 2014|POINT (-76.36255550435658 -38.115857985800844)|null
     6.  Clemens|5|Wed May 28 08:31:41 EDT 2014|POINT (-76.10591453732893 -38.85666776208012)|null
     7.  Addams|6|Thu Dec 18 06:31:37 EST 2014|POINT (-76.40849622595168 -38.268406683218565)|null
     8.  Bierce|7|Wed Oct 15 14:52:04 EDT 2014|POINT (-76.11437368081457 -37.10612535467402)|null
     9.  Clemens|8|Tue Feb 04 04:46:52 EST 2014|POINT (-77.26323925805912 -38.7179185982736)|null
     10.  Addams|9|Mon May 26 21:02:15 EDT 2014|POINT (-77.86991721689314 -38.958277119863986)|null
