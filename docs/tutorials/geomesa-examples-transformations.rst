GeoMesa Transformations Example
===============================

This tutorial shows how to use GeoTools to select and modify the attributes returned from a query.

About this Tutorial
-------------------

In the spirit of keeping things simple, the code in this tutorial only
does a few small things:

1. Demonstrate basic queries against GeoMesa
2. Demonstrate projection queries against GeoMesa
3. Demonstrate transformation queries against GeoMesa

These terms are potentially ambiguous, so we will describe them next.

Background
----------

GeoMesa allows users to perform `relational
projections <http://en.wikipedia.org/wiki/Projection_%28relational_algebra%29>`__
on query results. Projection is an overloaded term; when we use it here,
we refer to the relational sense. Although projections can also modify
an attribute's value, in this tutorial we will refer to such modifications as
transformations to keep things clearer.

Projections and transformations have the following uses and advantages:

1. Subset to specified columns - reduces network overhead of returning results
2. Rename specified columns - alters the schema of data on the fly
3. Compute new attributes from one or more original attributes - adds
   derived fields to results

When possible, projections and transformations are applied in parallel across the back-end cluster,
thus making them very fast. They are analogous to the map tasks in a
map-reduce job. Transformations are also extensible; developers can
implement new functions and plug them into the system using standard
mechanisms from `Geotools <http://www.geotools.org/>`__.

Choice of Backing Storage
-------------------------

This tutorial will work with several different back-ends. For simplicity, the rest of the tutorial will assume
the use of HBase. Alternatively, you may use Accumulo, Cassandra, Redis, or the GeoMesa FileSystem DataStore. If not
using HBase, the commands in the rest of the tutorial will vary slightly.

Prerequisites
-------------

Before you begin, you must have the following:

-  `Java <http://java.oracle.com/>`__ JDK 1.8
-  Apache `Maven <http://maven.apache.org/>`__ |maven_version|
-  a GitHub client
-  Completion of the GeoMesa quick start for your choice of back end

.. warning::

    This tutorial will use the data you wrote in the initial quick start, so make sure you complete that first.


Download and Build the Tutorial
-------------------------------

Pick a reasonable directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-tutorials.git
    $ cd geomesa-tutorials

.. warning::

    Make sure that you download or checkout the version of the tutorials project that corresponds to
    your GeoMesa version. See :ref:`tutorial_versions` for more details.

To ensure that the tutorial works with your environment, modify the ``pom.xml``
to set the appropriate versions for HBase, Hadoop, etc.

For ease of use, the project builds a bundled artifact that contains all the required
dependencies in a single JAR. To build, run:

.. code-block:: bash

    $ mvn clean install -pl geomesa-tutorials-hbase/geomesa-tutorials-hbase-transforms -am

.. note::

    The module name will vary depending on choice of back end.


Run the Tutorial
----------------

On the command-line, run:

.. code-block:: bash

    $ java -cp geomesa-tutorials-hbase/geomesa-tutorials-hbase-transforms/target/geomesa-tutorials-hbase-transforms-$VERSION.jar \
        org.geomesa.example.hbase.transformations.HBaseQueryTutorial \
        --hbase.zookeepers <zookeepers>                              \
        --hbase.catalog <table>

where you provide the following arguments:

-  ``<zookeepers>`` the HBase Zookeeper quorum. If you installed HBase in stand-alone mode,
   this will be ``localhost``. Note that for most use cases, it is preferable to put the
   ``hbase-site.xml`` from your cluster on the GeoMesa classpath instead of specifying Zookeepers.
-  ``<table>`` the name of the table that holds your quick-start data

.. note::

    The path, class name, and required arguments will vary depending on choice of back end.

The code will query GeoMesa using various projections and transforms and print out the results.

Looking at the Code
-------------------

The source code is meant to be accessible for this tutorial. The main logic is contained in
the generic ``org.geomesa.example.transformations.GeoMesaQueryTutorial`` in the ``geomesa-tutorials-common`` module,
which is datastore agnostic. Some relevant methods:


-  ``basicQuery`` executes a base filter without any further options.
   All attributes are returned in the data set.
-  ``basicProjectionQuery`` executes a base filter but specifies a
   subset of attributes to return.
-  ``basicTransformationQuery`` executes a base filter and transforms
   one of the attributes that is returned.
-  ``renamedTransformationQuery`` executes a base filter and transforms
   one of the attributes, returning it in a separate derived attribute.
-  ``mutliFieldTransformationQuery`` executes a base filter and
   transforms two attributes into a single derived attributes.
-  ``geometricTransformationQuery`` executes a base filter and
   transforms the geometry returned from a point into a polygon by
   buffering it.

Additional transformation functions are listed
`here <http://docs.geotools.org/latest/userguide/library/main/filter.html>`__.

*Please note that currently not all functions are supported by GeoMesa.*

Basic query with no projections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This query does not use any projections or transformations. Note that
all attributes are returned in the results.

.. code-block:: java

    Query query = new Query(simpleFeatureTypeName, cqlFilter);

Query with a projection for two attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This query uses a projection to only return the 'Actor1Name' and 'geom'
attributes.

.. code-block:: java

    String[] properties = new String[] { "Actor1Name", "geom" };
    Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

**Sample Output**

+-----------------+---------------------------+
| Actor1Name      | geom                      |
+=================+===========================+
| UNITED STATES   | POINT (32 49)             |
+-----------------+---------------------------+

Query with an attribute transformation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This query performs a transformation on the 'Actor1Name' attribute, to
print it in a more user-friendly format.

.. code-block:: java

    String[] properties = new String[] { "geom", "Actor1Name=strCapitalize(Actor1Name)" };
    Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

**Sample Output**

+---------------------------+-----------------+
| geom                      | Actor1Name      |
+===========================+=================+
| POINT (30.5167 50.4333)   | United States   |
+---------------------------+-----------------+

Query with a derived attribute
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This query creates a new attribute called 'derived' based off a join of
the 'Actor1Name' and 'Actor1Geo_FullName' attribute. This could be used
to show the actor and location of the event, for example.

.. code-block:: java

    String property = "derived=strConcat(Actor1Name,strConcat(' - ',Actor1Geo_FullName))";
    String[] properties = new String[] { geom, property };
    Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

**Sample Output**

+---------------------------+-----------------------------------------------------+
| geom                      | derived                                             |
+===========================+=====================================================+
| POINT (30.5167 50.4333)   | UNITED STATES - Kyiv, Kyyiv, Misto, Ukraine         |
+---------------------------+-----------------------------------------------------+

Query with a geometric transformation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This query performs a geometric transformation on the points returned,
buffering them by a fixed amount. This could be used to estimate an area
of impact around a particular event, for example.

.. code-block:: java

    String[] properties = new String[] { "geom", "derived=buffer(geom, 2)" };
    Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

**Sample Output**

+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| geom                      | derived                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
+===========================+========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================+
| POINT (30.5167 50.4333)   | POLYGON ((32.5167 50.4333, 32.478270560806465 50.04311935596775, 32.36445906502257 49.66793313526982, 32.17963922460509 49.3221595339608, 31.930913562373096 49.01908643762691, 31.627840466039206 48.77036077539491, 31.28206686473018 48.58554093497743, 30.906880644032256 48.47172943919354, 30.5167 48.4333, 30.126519355967744 48.47172943919354, 29.75133313526982 48.58554093497743, 29.405559533960798 48.77036077539491, 29.102486437626904 49.01908643762691, 28.85376077539491 49.3221595339608, 28.668940934977428 49.66793313526983, 28.55512943919354 50.04311935596775, 28.5167 50.4333, 28.55512943919354 50.82348064403226, 28.668940934977428 51.198666864730185, 28.85376077539491 51.54444046603921, 29.102486437626908 51.8475135623731, 29.405559533960798 52.09623922460509, 29.751333135269824 52.281059065022575, 30.126519355967748 52.39487056080647, 30.516700000000004 52.4333, 30.906880644032263 52.39487056080646, 31.282066864730186 52.281059065022575, 31.62784046603921 52.09623922460509, 31.9309135623731 51.847513562373095, 32.1796392246051 51.5444404660392, 32.36445906502258 51.19866686473018, 32.478270560806465 50.82348064403225, 32.5167 50.4333))   |
+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
