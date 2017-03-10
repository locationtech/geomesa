Tutorials
=========

Getting Started
---------------

.. toctree::
    :maxdepth: 1

    geomesa-quickstart-accumulo
    geomesa-quickstart-kafka
    geomesa-quickstart-hbase
    geodocker-geomesa-spark-on-aws
    geomesa-hbase-s3-on-aws

Data In/Out
-----------

.. toctree::
    :maxdepth: 1

    geomesa-quickstart-storm
    geomesa-examples-gdelt
    geomesa-examples-avro
    geomesa-raster
    geomesa-examples-transformations
    geomesa-blobstore
    geomesa-blobstore-exif

Data Analysis
-------------

.. toctree::
    :maxdepth: 1

    spark
    shallow-join
    geomesa-tubeselect

.. _accumulo_tutorials_security:

Security
--------

.. toctree::
    :maxdepth: 1

    geomesa-examples-authorizations
    geomesa-examples-featurelevelvis

Indexing and Queries
--------------------

.. toctree::
    :maxdepth: 1

    geohash-substrings


.. _tutorial_versions:

About Tutorial Versions
-----------------------

The tutorials listed in this manual can be obtained from GitHub, by
cloning the `geomesa-tutorials`_ project:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-tutorials.git
    $ cd geomesa-tutorials

Keep in mind that you may have to download a particular release of
the tutorials project to match the GeoMesa version that
you are using. For example, to target GeoMesa |release|, you should download
version |release_tutorial| of the `geomesa-tutorials`_ project
(``$TUTORIAL_VERSION`` = |release_tutorial|):

.. code-block:: bash

    $ git checkout geomesa-tutorials-$TUTORIAL_VERSION

In general, the major, minor, and patch version numbers of the
tutorials release will match the corresponding numbers of the
GeoMesa version. The tutorials version contains a fourth digit
number permitting multiple releases per GeoMesa release.

You may also see the `geomesa-tutorials`_ releases available, and
download a tarball of a release on the `geomesa-tutorials releases page`_.

.. _geomesa-tutorials: https://github.com/geomesa/geomesa-tutorials/

.. _geomesa-tutorials releases page: https://github.com/geomesa/geomesa-tutorials/releases

Hadoop Version
^^^^^^^^^^^^^^

Most of the tutorials encourage you to update the ``pom.xml``
to match the versions of the services you are using (Hadoop,
ZooKeeper, Accumulo, etc.) However, there may be issues when
incrementing the Hadoop version to 2.6 or above, which can result
in Apache Curator version conflicts. Leaving the
Hadoop version set to 2.2 in the tutorials ``pom.xml`` will work
with all subsequent Hadoop 2.X releases.
