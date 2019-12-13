OpenStreetMap Data
==================

`OpenStreetMap <http://wiki.openstreetmap.org/wiki/Main_Page>`__ is a free, editable map of the whole world that is
being built by volunteers largely from scratch and released with an open-content license.

Getting OSM data
----------------

Various slices of data are available to download `here <https://planet.osm.org/>`__  in either XML or
the binary PBF format.

Nodes vs Ways
-------------

OSM files include two data types - nodes and ways. Nodes are point features; ways are line strings composed of several
nodes. In order to ingest ways, the relevant nodes must be stored temporarily. By default, the OSM converter will
use a file-based H2 database for storing nodes. This may not be sufficient for large files, however. See
`Ingest Configuration`_ below for details.

Tags
----

OSM tags are stored in a single field as JSON. See :ref:`json_attributes` for details on querying and selecting
individual tags.

Installing OSM Converter
------------------------

The OSM converter does not ship with the GeoMesa tools by default, due to some dependency conflicts. To install
it currently requires the source distribution and Maven. Download or clone the project from
`Github <https://github.com/locationtech/geomesa>`__. Ensure that the version matches your GeoMesa tools
version, either by checking out the correct Git branch, or by downloading the appropriate tag. Run the following
commands:

::

    mvn clean install -DskipTests
    mvn dependency:copy-dependencies -pl geomesa-convert/geomesa-convert-osm/ \
      -DincludeScope=compile -DoutputDirectory=$GEOMESA_HOME/lib
    cp geomesa-convert/geomesa-convert-osm/target/geomesa-convert-osm_2.11-$version.jar \
      $GEOMESA_HOME/lib


Check that the ``osm-nodes`` and ``osm-ways`` simple feature types are available on the GeoMesa
tools classpath:

::

    $ geomesa-accumulo env | grep osm-nodes
    $ geomesa-accumulo env | grep osm-ways

Ingest Configuration
--------------------

The OSM converters and simple feature types are defined in ``$GEOMESA_HOME/conf/sfts/osm/reference.conf``. To ingest
XML files instead of PBF, you will need to change the ``format`` field to ``xml``. For ingesting OSM ways,
you may also specify a JDBC connection string for storing nodes. Note that you will need the appropriate JDBC
driver JAR in ``$GEOMESA_HOME/lib``

Ingest Commands
---------------

Run the ingest. You may optionally point to a different accumulo
instance using ``-i`` and ``-z`` options. See ``geomesa-accumulo help ingest``
for more detail.

::

    $ geomesa-accumulo ingest -u USERNAME -c CATALOGNAME -s osm-nodes -C osm-nodes test.osm

