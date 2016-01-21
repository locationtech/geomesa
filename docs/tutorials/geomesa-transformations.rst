GeoMesa Transformations
=======================

This tutorial will show you how to write custom Java code using GeoMesa to do the following:

1. Query previously-ingested data.
2. Apply `relational
   projections <http://en.wikipedia.org/wiki/Projection_%28relational_algebra%29>`__
   to your query results.
3. Apply transformations to your query results.

Background
----------

GeoMesa allows users to perform `relational
projections <http://en.wikipedia.org/wiki/Projection_%28relational_algebra%29>`__
on query results. We call these "transformations" to distinguish them
from the overloaded term "projection" which has a different meaning in a
spatial context. These transformations have the following uses and
advantages:

1. Subset to specified columns - reduces network overhead of returning
   results
2. Rename specified columns - alters the schema of data on the fly
3. Compute new attributes from one or more original attributes - adds
   derived fields to results

The transformations are applied in parallel across the cluster thus
making them very fast. They are analogous to the map tasks in a
map-reduce job. Transformations are also extensible; developers can
implement new functions and plug them into the system using standard
mechanisms from `GeoTools <http://www.geotools.org/>`__.

**Note:** when this tutorial refers to "projections", it means in the
relational sense - see `Projection - Relational
Algebra <http://en.wikipedia.org/wiki/Projection_(relational_algebra)>`__.
Projection also has `many other
meanings <http://en.wikipedia.org/wiki/Projection_(disambiguation)>`__
in spatial discussions - they are not used in this tutorial. Although
projections can also modify an attribute's value, in this tutorial we
will refer to such modifications as "transformations" to keep things
clearer.

Prerequisites
-------------

.. raw:: html

   <div class="callout callout-warning">

::

    <span class="glyphicon glyphicon-exclamation-sign"></span>
    You will need access to a Hadoop 2.2 installation as well as an Accumulo |accumulo_version| database.

.. raw:: html

   </div>

.. raw:: html

   <div class="callout callout-warning">

::

    <span class="glyphicon glyphicon-exclamation-sign"></span>
    You will need to have ingested GDELT data using GeoMesa. Instructions are available <a href="/geomesa-gdelt-analysis/">here</a>.

.. raw:: html

   </div>

You will also need:

-  an Accumulo user that has appropriate permissions to query your data,
-  Java JDK 7,
-  `Apache Maven <http://maven.apache.org/>`__ 3.2.2 or better, and
-  a `git <http://git-scm.com/>`__ client.

Download and Build Tutorial Code
--------------------------------

Clone the geomesa project and build it, if you haven't already:

.. code:: bash

    $ git clone https://github.com/locationtech/geomesa.git
    $ cd geomesa
    $ mvn clean install

This is needed to install the GeoMesa JAR files in your local Maven
repository. For more information see the `GeoMesa Accumulo Quick
Start </geomesa-quickstart/>`__ tutorial.

Clone the GeoMesa tutorials project:

.. code:: bash

    $ git clone https://github.com/geomesa/geomesa-tutorials.git

The source code for this tutorial is in the ``geomesa-examples-transformations``
directory:

    $ cd geomesa-tutorials/geomesa-examples-transformations

The Maven ``pom.xml`` file contains an explicit list of dependent
libraries that will be bundled together into the final tutorial. You
should confirm that the versions of Accumulo and Hadoop match what you
are running; if it does not match, change the value in the POM.

.. note::

    The only reason these libraries are bundled into the final JAR is that this
    is easier for most people to do this than it is to set the classpath
    when running the tutorial. If you would rather not bundle these
    dependencies, mark them as provided in the POM, and update your
    classpath as appropriate.

From within this directory, run:

.. code:: bash

    $ mvn clean install

When this is complete, it will have built a JAR file that contains all
of the code you need to run the tutorial in the ``target`` subdirectory.

Run the Tutorial
----------------

You will need to have ingested some GDELT data using GeoMesa;
instructions are available in the `GDELT Map-Reduce
tutorial </geomesa-gdelt-analysis/>`__. Ideally data spanning 2013-2014
should be included, as this tutorial follows the previous tutorial in
searching for events in the Ukraine during the recent civil unrest.

On the command line, run:

.. code:: bash

    java -cp ./target/geomesa-examples-transformations-$VERSION.jar \
       com.example.geomesa.transformations.QueryTutorial \
       -instanceId <instance> \
       -zookeepers <zoos> \
       -user <user> \
       -password <pwd> \
       -tableName <table> \
       -featureName <feature>

where you provide the following arguments:

-  ``<instance>`` - the name of your Accumulo instance
-  ``<zoos>`` - comma-separated list of your Zookeeper nodes, e.g.
   zoo1:2181,zoo2:2181,zoo3:2181
-  ``<user>`` - the name of an Accumulo user that will execute the
   scans, e.g. root
-  ``<pwd>`` - the password for the previously-mentioned Accumulo user
-  ``<table>`` - the name of the Accumulo table that has the GeoMesa
   GDELT dataset, e.g. "gdelt" if you followed the GDELT tutorial
-  ``<feature>`` - the feature name used to ingest the GeoMesa GDELT
   dataset, e.g. "event" if you followed the GDELT tutorial

You should see several queries run and the results printed out to your
console.

Looking Closer at the Code
--------------------------

The code for querying and projections is available in the class
``QueryTutorial``. The source code is meant to be
accessible, but here is a high-level breakdown of the relevant methods:

-  ``basicQuery`` - executes a base filter without any further options.
   All attributes are returned in the data set.
-  ``basicProjectionQuery`` - executes a base filter but specifies a
   subset of attributes to return.
-  ``basicTransformationQuery`` - executes a base filter and transforms
   one of the attributes that is returned.
-  ``renamedTransformationQuery`` - executes a base filter and
   transforms one of the attributes, returning it in a separate derived
   attribute.
-  ``mutliFieldTransformationQuery`` - executes a base filter and
   transforms two attributes into a single derived attributes.
-  ``geometricTransformationQuery`` - executes a base filter and
   transforms the geometry returned from a point into a polygon by
   buffering it.

Additional transformation functions are listed
`here <http://docs.geotools.org/latest/userguide/library/main/filter.html>`__.
*Please note that currently not all functions are supported by GeoMesa.*

Additionally, there are two helper classes included in the tutorial:

-  ``GdeltFeature`` - Contains the properties
   (attributes) available in the GDELT data set.
-  ``SetupUtil`` - Handles reading command-line
   arguments.

Sample Code and Output
----------------------

The following code snippets show the basic aspects of creating queries
for GeoMesa.

.. raw:: html

   <style>
     div.output-scroll {
       margin-left: 30px;
       overflow: auto;
       width: 90%;
     }
     table.output {
       border: 2px inset white;
     }
     table.output td {
       font-size: 12px;
     }
     table.output th, table.output td {
       padding: 5px 10px;
       white-space: nowrap;
     }
     table.output tr.odd th, table.output tr.odd td {
       background-color: gray;
       color: black;
     }
   </style>

Create a basic query with no projections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This query does not use any projections or transformations. Note that
all attributes are returned in the results.

.. code:: java

    Query query = new Query(simpleFeatureTypeName, cqlFilter);

**Output**

.. raw:: html

   <div class="output-scroll">

.. raw:: html

   <table class="output">

.. raw:: html

   <tr>

.. raw:: html

   <th>

Result

.. raw:: html

   </th>

.. raw:: html

   <th>

GLOBALEVENTID

.. raw:: html

   </th>

.. raw:: html

   <th>

SQLDATE

.. raw:: html

   </th>

.. raw:: html

   <th>

MonthYear

.. raw:: html

   </th>

.. raw:: html

   <th>

Year

.. raw:: html

   </th>

.. raw:: html

   <th>

FractionDate

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Name

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1CountryCode

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1KnownGroupCode

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1EthnicCode

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Religion1Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Religion2Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Type1Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Type2Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Type3Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Name

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2CountryCode

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2KnownGroupCode

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2EthnicCode

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Religion1Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Religion2Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Type1Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Type2Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Type3Code

.. raw:: html

   </th>

.. raw:: html

   <th>

IsRootEvent

.. raw:: html

   </th>

.. raw:: html

   <th>

EventCode

.. raw:: html

   </th>

.. raw:: html

   <th>

EventBaseCode

.. raw:: html

   </th>

.. raw:: html

   <th>

EventRootCode

.. raw:: html

   </th>

.. raw:: html

   <th>

QuadClass

.. raw:: html

   </th>

.. raw:: html

   <th>

GoldsteinScale

.. raw:: html

   </th>

.. raw:: html

   <th>

NumMentions

.. raw:: html

   </th>

.. raw:: html

   <th>

NumSources

.. raw:: html

   </th>

.. raw:: html

   <th>

NumArticles

.. raw:: html

   </th>

.. raw:: html

   <th>

AvgTone

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Geo\_Type

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Geo\_FullName

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Geo\_CountryCode

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Geo\_ADM1Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Geo\_Lat

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Geo\_Long

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Geo\_FeatureID

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Geo\_Type

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Geo\_FullName

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Geo\_CountryCode

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Geo\_ADM1Code

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Geo\_Lat

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Geo\_Long

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor2Geo\_FeatureID

.. raw:: html

   </th>

.. raw:: html

   <th>

ActionGeo\_Type

.. raw:: html

   </th>

.. raw:: html

   <th>

ActionGeo\_FullName

.. raw:: html

   </th>

.. raw:: html

   <th>

ActionGeo\_CountryCode

.. raw:: html

   </th>

.. raw:: html

   <th>

ActionGeo\_ADM1Code

.. raw:: html

   </th>

.. raw:: html

   <th>

ActionGeo\_Lat

.. raw:: html

   </th>

.. raw:: html

   <th>

ActionGeo\_Long

.. raw:: html

   </th>

.. raw:: html

   <th>

ActionGeo\_FeatureID

.. raw:: html

   </th>

.. raw:: html

   <th>

DATEADDED

.. raw:: html

   </th>

.. raw:: html

   <th>

geom

.. raw:: html

   </th>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

284464526

.. raw:: html

   </td>

.. raw:: html

   <td>

Sun Feb 02 00:00:00 EST 2014

.. raw:: html

   </td>

.. raw:: html

   <td>

201402

.. raw:: html

   </td>

.. raw:: html

   <td>

2014

.. raw:: html

   </td>

.. raw:: html

   <td>

2014.0876

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

USAGOV

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

GOV

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

0

.. raw:: html

   </td>

.. raw:: html

   <td>

010

.. raw:: html

   </td>

.. raw:: html

   <td>

010

.. raw:: html

   </td>

.. raw:: html

   <td>

01

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

0.0

.. raw:: html

   </td>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

2.6362038

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

Kyiv, Kyyiv, Misto, Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP12

.. raw:: html

   </td>

.. raw:: html

   <td>

50.4333

.. raw:: html

   </td>

.. raw:: html

   <td>

30.5167

.. raw:: html

   </td>

.. raw:: html

   <td>

-1044367

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

United States

.. raw:: html

   </td>

.. raw:: html

   <td>

US

.. raw:: html

   </td>

.. raw:: html

   <td>

US

.. raw:: html

   </td>

.. raw:: html

   <td>

38.0

.. raw:: html

   </td>

.. raw:: html

   <td>

-97.0

.. raw:: html

   </td>

.. raw:: html

   <td>

null

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

United States

.. raw:: html

   </td>

.. raw:: html

   <td>

US

.. raw:: html

   </td>

.. raw:: html

   <td>

US

.. raw:: html

   </td>

.. raw:: html

   <td>

38.0

.. raw:: html

   </td>

.. raw:: html

   <td>

-97.0

.. raw:: html

   </td>

.. raw:: html

   <td>

null

.. raw:: html

   </td>

.. raw:: html

   <td>

20140202

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

284466704

.. raw:: html

   </td>

.. raw:: html

   <td>

Sun Feb 02 00:00:00 EST 2014

.. raw:: html

   </td>

.. raw:: html

   <td>

201402

.. raw:: html

   </td>

.. raw:: html

   <td>

2014

.. raw:: html

   </td>

.. raw:: html

   <td>

2014.0876

.. raw:: html

   </td>

.. raw:: html

   <td>

USAGOV

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

GOV

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

036

.. raw:: html

   </td>

.. raw:: html

   <td>

036

.. raw:: html

   </td>

.. raw:: html

   <td>

03

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

4.0

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

1.5810276

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

49.0

.. raw:: html

   </td>

.. raw:: html

   <td>

32.0

.. raw:: html

   </td>

.. raw:: html

   <td>

null

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

49.0

.. raw:: html

   </td>

.. raw:: html

   <td>

32.0

.. raw:: html

   </td>

.. raw:: html

   <td>

null

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

49.0

.. raw:: html

   </td>

.. raw:: html

   <td>

32.0

.. raw:: html

   </td>

.. raw:: html

   <td>

null

.. raw:: html

   </td>

.. raw:: html

   <td>

20140202

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (32 49)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

3

.. raw:: html

   </td>

.. raw:: html

   <td>

284427971

.. raw:: html

   </td>

.. raw:: html

   <td>

Sun Feb 02 00:00:00 EST 2014

.. raw:: html

   </td>

.. raw:: html

   <td>

201402

.. raw:: html

   </td>

.. raw:: html

   <td>

2014

.. raw:: html

   </td>

.. raw:: html

   <td>

2014.0876

.. raw:: html

   </td>

.. raw:: html

   <td>

IGOUNO

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED NATIONS

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

UNO

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

IGO

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

0

.. raw:: html

   </td>

.. raw:: html

   <td>

012

.. raw:: html

   </td>

.. raw:: html

   <td>

012

.. raw:: html

   </td>

.. raw:: html

   <td>

01

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

-0.4

.. raw:: html

   </td>

.. raw:: html

   <td>

27

.. raw:: html

   </td>

.. raw:: html

   <td>

3

.. raw:: html

   </td>

.. raw:: html

   <td>

27

.. raw:: html

   </td>

.. raw:: html

   <td>

1.0064903

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

Kiev, Ukraine (general), Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP00

.. raw:: html

   </td>

.. raw:: html

   <td>

50.4333

.. raw:: html

   </td>

.. raw:: html

   <td>

30.5167

.. raw:: html

   </td>

.. raw:: html

   <td>

-1044367

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

Kiev, Ukraine (general), Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP00

.. raw:: html

   </td>

.. raw:: html

   <td>

50.4333

.. raw:: html

   </td>

.. raw:: html

   <td>

30.5167

.. raw:: html

   </td>

.. raw:: html

   <td>

-1044367

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

Kiev, Ukraine (general), Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP00

.. raw:: html

   </td>

.. raw:: html

   <td>

50.4333

.. raw:: html

   </td>

.. raw:: html

   <td>

30.5167

.. raw:: html

   </td>

.. raw:: html

   <td>

-1044367

.. raw:: html

   </td>

.. raw:: html

   <td>

20140202

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

284466607

.. raw:: html

   </td>

.. raw:: html

   <td>

Sun Feb 02 00:00:00 EST 2014

.. raw:: html

   </td>

.. raw:: html

   <td>

201402

.. raw:: html

   </td>

.. raw:: html

   <td>

2014

.. raw:: html

   </td>

.. raw:: html

   <td>

2014.0876

.. raw:: html

   </td>

.. raw:: html

   <td>

USAGOV

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

GOV

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

UKR

.. raw:: html

   </td>

.. raw:: html

   <td>

UKRAINE

.. raw:: html

   </td>

.. raw:: html

   <td>

UKR

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

100

.. raw:: html

   </td>

.. raw:: html

   <td>

100

.. raw:: html

   </td>

.. raw:: html

   <td>

10

.. raw:: html

   </td>

.. raw:: html

   <td>

3

.. raw:: html

   </td>

.. raw:: html

   <td>

-5.0

.. raw:: html

   </td>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

7.826087

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

49.0

.. raw:: html

   </td>

.. raw:: html

   <td>

32.0

.. raw:: html

   </td>

.. raw:: html

   <td>

null

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

49.0

.. raw:: html

   </td>

.. raw:: html

   <td>

32.0

.. raw:: html

   </td>

.. raw:: html

   <td>

null

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

49.0

.. raw:: html

   </td>

.. raw:: html

   <td>

32.0

.. raw:: html

   </td>

.. raw:: html

   <td>

null

.. raw:: html

   </td>

.. raw:: html

   <td>

20140202

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (32 49)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

5

.. raw:: html

   </td>

.. raw:: html

   <td>

284464187

.. raw:: html

   </td>

.. raw:: html

   <td>

Sun Feb 02 00:00:00 EST 2014

.. raw:: html

   </td>

.. raw:: html

   <td>

201402

.. raw:: html

   </td>

.. raw:: html

   <td>

2014

.. raw:: html

   </td>

.. raw:: html

   <td>

2014.0876

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

USA

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

UKR

.. raw:: html

   </td>

.. raw:: html

   <td>

UKRAINE

.. raw:: html

   </td>

.. raw:: html

   <td>

UKR

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

.. raw:: html

   </td>

.. raw:: html

   <td>

0

.. raw:: html

   </td>

.. raw:: html

   <td>

111

.. raw:: html

   </td>

.. raw:: html

   <td>

111

.. raw:: html

   </td>

.. raw:: html

   <td>

11

.. raw:: html

   </td>

.. raw:: html

   <td>

3

.. raw:: html

   </td>

.. raw:: html

   <td>

-2.0

.. raw:: html

   </td>

.. raw:: html

   <td>

5

.. raw:: html

   </td>

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

5

.. raw:: html

   </td>

.. raw:: html

   <td>

1.4492754

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

Kiev, Ukraine (general), Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP00

.. raw:: html

   </td>

.. raw:: html

   <td>

50.4333

.. raw:: html

   </td>

.. raw:: html

   <td>

30.5167

.. raw:: html

   </td>

.. raw:: html

   <td>

-1044367

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

Kiev, Ukraine (general), Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP00

.. raw:: html

   </td>

.. raw:: html

   <td>

50.4333

.. raw:: html

   </td>

.. raw:: html

   <td>

30.5167

.. raw:: html

   </td>

.. raw:: html

   <td>

-1044367

.. raw:: html

   </td>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

Kiev, Ukraine (general), Ukraine

.. raw:: html

   </td>

.. raw:: html

   <td>

UP

.. raw:: html

   </td>

.. raw:: html

   <td>

UP00

.. raw:: html

   </td>

.. raw:: html

   <td>

50.4333

.. raw:: html

   </td>

.. raw:: html

   <td>

30.5167

.. raw:: html

   </td>

.. raw:: html

   <td>

-1044367

.. raw:: html

   </td>

.. raw:: html

   <td>

20140202

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   </table>

.. raw:: html

   </div>

Create a query with a projection for two attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This query uses a projection to only return the 'Actor1Name' and 'geom'
attributes.

.. code:: java

    String[] properties = new String[] {"Actor1Name", "geom"};
    Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

**Output**

.. raw:: html

   <div class="output-scroll">

.. raw:: html

   <table class="output">

.. raw:: html

   <tr>

.. raw:: html

   <th>

Result

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Name

.. raw:: html

   </th>

.. raw:: html

   <th>

geom

.. raw:: html

   </th>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (32 49)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

3

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

5

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   </table>

.. raw:: html

   </div>

Create a query with an attribute transformation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This query performs a transformation on the 'Actor1Name' attribute, to
print it in a more user-friendly format.

.. code:: java

    String[] properties = new String[] {"Actor1Name=strCapitalize(Actor1Name)", "geom"};
    Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

**Output**

.. raw:: html

   <div class="output-scroll">

.. raw:: html

   <table class="output">

.. raw:: html

   <tr>

.. raw:: html

   <th>

Result

.. raw:: html

   </th>

.. raw:: html

   <th>

geom

.. raw:: html

   </th>

.. raw:: html

   <th>

Actor1Name

.. raw:: html

   </th>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

United States

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (32 49)

.. raw:: html

   </td>

.. raw:: html

   <td>

United States

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

3

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (32 49)

.. raw:: html

   </td>

.. raw:: html

   <td>

United States

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

United States

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

5

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

United States

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   </table>

.. raw:: html

   </div>

Create a query with a derived attribute
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This query creates a new attribute called 'derived' based off a join of
the 'Actor1Name' and 'Actor1Geo\_FullName' attribute. This could be used
to show the actor and location of the event, for example.

.. code:: java

    String[] properties = new String[] {"derived=strConcat(Actor1Name,strConcat(' - ',Actor1Geo_FullName)),geom"};
    Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

**Output**

.. raw:: html

   <div class="output-scroll">

.. raw:: html

   <table class="output">

.. raw:: html

   <tr>

.. raw:: html

   <th>

Result

.. raw:: html

   </th>

.. raw:: html

   <th>

geom

.. raw:: html

   </th>

.. raw:: html

   <th>

derived

.. raw:: html

   </th>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES - Kyiv, Kyyiv, Misto, Ukraine

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (32 49)

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES - Ukraine

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

3

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES - Kiev, Ukraine (general), Ukraine

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (32 49)

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED STATES - Ukraine

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

5

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

UNITED NATIONS - Kiev, Ukraine (general), Ukraine

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   </table>

.. raw:: html

   </div>

Create a query with a geometric transformation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This query performs a geometric transformation on the points returned,
buffering them by a fixed amount. This could be used to estimate an area
of impact around a particular event, for example.

.. code:: java

    String[] properties = new String[] {"geom,derived=buffer(geom, 2)"};
    Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

**Output**

.. raw:: html

   <div class="output-scroll">

.. raw:: html

   <table class="output">

.. raw:: html

   <tr>

.. raw:: html

   <th>

Result

.. raw:: html

   </th>

.. raw:: html

   <th>

geom

.. raw:: html

   </th>

.. raw:: html

   <th>

derived

.. raw:: html

   </th>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

1

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

POLYGON ((32.5167 50.4333, 32.478270560806465 50.04311935596775,
32.36445906502257 49.66793313526982, 32.17963922460509 49.3221595339608,
31.930913562373096 49.01908643762691, 31.627840466039206
48.77036077539491, 31.28206686473018 48.58554093497743,
30.906880644032256 48.47172943919354, 30.5167 48.4333,
30.126519355967744 48.47172943919354, 29.75133313526982
48.58554093497743, 29.405559533960798 48.77036077539491,
29.102486437626904 49.01908643762691, 28.85376077539491
49.3221595339608, 28.668940934977428 49.66793313526983,
28.55512943919354 50.04311935596775, 28.5167 50.4333, 28.55512943919354
50.82348064403226, 28.668940934977428 51.198666864730185,
28.85376077539491 51.54444046603921, 29.102486437626908
51.8475135623731, 29.405559533960798 52.09623922460509,
29.751333135269824 52.281059065022575, 30.126519355967748
52.39487056080647, 30.516700000000004 52.4333, 30.906880644032263
52.39487056080646, 31.282066864730186 52.281059065022575,
31.62784046603921 52.09623922460509, 31.9309135623731
51.847513562373095, 32.1796392246051 51.5444404660392, 32.36445906502258
51.19866686473018, 32.478270560806465 50.82348064403225, 32.5167
50.4333))

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

2

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

POLYGON ((32.5167 50.4333, 32.478270560806465 50.04311935596775,
32.36445906502257 49.66793313526982, 32.17963922460509 49.3221595339608,
31.930913562373096 49.01908643762691, 31.627840466039206
48.77036077539491, 31.28206686473018 48.58554093497743,
30.906880644032256 48.47172943919354, 30.5167 48.4333,
30.126519355967744 48.47172943919354, 29.75133313526982
48.58554093497743, 29.405559533960798 48.77036077539491,
29.102486437626904 49.01908643762691, 28.85376077539491
49.3221595339608, 28.668940934977428 49.66793313526983,
28.55512943919354 50.04311935596775, 28.5167 50.4333, 28.55512943919354
50.82348064403226, 28.668940934977428 51.198666864730185,
28.85376077539491 51.54444046603921, 29.102486437626908
51.8475135623731, 29.405559533960798 52.09623922460509,
29.751333135269824 52.281059065022575, 30.126519355967748
52.39487056080647, 30.516700000000004 52.4333, 30.906880644032263
52.39487056080646, 31.282066864730186 52.281059065022575,
31.62784046603921 52.09623922460509, 31.9309135623731
51.847513562373095, 32.1796392246051 51.5444404660392, 32.36445906502258
51.19866686473018, 32.478270560806465 50.82348064403225, 32.5167
50.4333))

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

3

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (32 49)

.. raw:: html

   </td>

.. raw:: html

   <td>

POLYGON ((34 49, 33.961570560806464 48.609819355967744,
33.84775906502257 48.23463313526982, 33.66293922460509 47.8888595339608,
33.41421356237309 47.58578643762691, 33.1111404660392 47.33706077539491,
32.76536686473018 47.15224093497743, 32.390180644032256
47.038429439193536, 32 47, 31.609819355967744 47.038429439193536,
31.23463313526982 47.15224093497743, 30.888859533960797
47.33706077539491, 30.585786437626904 47.58578643762691,
30.33706077539491 47.8888595339608, 30.152240934977428
48.234633135269824, 30.03842943919354 48.609819355967744, 30 49,
30.03842943919354 49.390180644032256, 30.152240934977428
49.76536686473018, 30.33706077539491 50.11114046603921,
30.585786437626908 50.4142135623731, 30.888859533960797
50.66293922460509, 31.234633135269824 50.84775906502257,
31.609819355967748 50.961570560806464, 32.00000000000001 51,
32.39018064403226 50.96157056080646, 32.76536686473018
50.84775906502257, 33.11114046603921 50.66293922460509, 33.4142135623731
50.41421356237309, 33.6629392246051 50.111140466039195,
33.84775906502258 49.765366864730176, 33.961570560806464
49.39018064403225, 34 49))

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr>

.. raw:: html

   <td>

4

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

POLYGON ((32.5167 50.4333, 32.478270560806465 50.04311935596775,
32.36445906502257 49.66793313526982, 32.17963922460509 49.3221595339608,
31.930913562373096 49.01908643762691, 31.627840466039206
48.77036077539491, 31.28206686473018 48.58554093497743,
30.906880644032256 48.47172943919354, 30.5167 48.4333,
30.126519355967744 48.47172943919354, 29.75133313526982
48.58554093497743, 29.405559533960798 48.77036077539491,
29.102486437626904 49.01908643762691, 28.85376077539491
49.3221595339608, 28.668940934977428 49.66793313526983,
28.55512943919354 50.04311935596775, 28.5167 50.4333, 28.55512943919354
50.82348064403226, 28.668940934977428 51.198666864730185,
28.85376077539491 51.54444046603921, 29.102486437626908
51.8475135623731, 29.405559533960798 52.09623922460509,
29.751333135269824 52.281059065022575, 30.126519355967748
52.39487056080647, 30.516700000000004 52.4333, 30.906880644032263
52.39487056080646, 31.282066864730186 52.281059065022575,
31.62784046603921 52.09623922460509, 31.9309135623731
51.847513562373095, 32.1796392246051 51.5444404660392, 32.36445906502258
51.19866686473018, 32.478270560806465 50.82348064403225, 32.5167
50.4333))

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   <tr class="odd">

.. raw:: html

   <td>

5

.. raw:: html

   </td>

.. raw:: html

   <td>

POINT (30.5167 50.4333)

.. raw:: html

   </td>

.. raw:: html

   <td>

POLYGON ((32.5167 50.4333, 32.478270560806465 50.04311935596775,
32.36445906502257 49.66793313526982, 32.17963922460509 49.3221595339608,
31.930913562373096 49.01908643762691, 31.627840466039206
48.77036077539491, 31.28206686473018 48.58554093497743,
30.906880644032256 48.47172943919354, 30.5167 48.4333,
30.126519355967744 48.47172943919354, 29.75133313526982
48.58554093497743, 29.405559533960798 48.77036077539491,
29.102486437626904 49.01908643762691, 28.85376077539491
49.3221595339608, 28.668940934977428 49.66793313526983,
28.55512943919354 50.04311935596775, 28.5167 50.4333, 28.55512943919354
50.82348064403226, 28.668940934977428 51.198666864730185,
28.85376077539491 51.54444046603921, 29.102486437626908
51.8475135623731, 29.405559533960798 52.09623922460509,
29.751333135269824 52.281059065022575, 30.126519355967748
52.39487056080647, 30.516700000000004 52.4333, 30.906880644032263
52.39487056080646, 31.282066864730186 52.281059065022575,
31.62784046603921 52.09623922460509, 31.9309135623731
51.847513562373095, 32.1796392246051 51.5444404660392, 32.36445906502258
51.19866686473018, 32.478270560806465 50.82348064403225, 32.5167
50.4333))

.. raw:: html

   </td>

.. raw:: html

   </tr>

.. raw:: html

   </table>

.. raw:: html

   </div>
