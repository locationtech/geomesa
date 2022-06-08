Installing GeoMesa HBase
========================

.. note::

    GeoMesa currently supports HBase |hbase_supported_versions|.

.. note::

    The examples below expect a version to be set in the environment:

    .. parsed-literal::

        $ export TAG="|release_version|"
<<<<<<< HEAD
        $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        # note: |scala_binary_version| is the Scala build version
        $ export VERSION="|scala_binary_version|-${TAG}"
<<<<<<< HEAD
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        # note: |scala_binary_version| is the Scala build version
        $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)

GeoMesa supports traditional HBase installations as well as HBase running on `Amazon's EMR <https://aws.amazon.com/emr/>`_
, `Hortonworks' Data Platform (HDP) <https://hortonworks.com/products/data-center/hdp/>`_, and the
`Cloudera Distribution of Hadoop (CDH) <https://www.cloudera.com/products/enterprise-data-hub.html>`_. For details
on bootstrapping an EMR cluster, see :doc:`/tutorials/geomesa-hbase-s3-on-aws`. For details on deploying to
Cloudera CDH, see :doc:`/tutorials/geomesa-hbase-on-cdh`.

Installing the Binary Distribution
----------------------------------

GeoMesa HBase artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
Download and extract it somewhere convenient:
=======
.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).

Extract it somewhere convenient:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
Download and extract it somewhere convenient:
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
Download and extract it somewhere convenient:
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-hbase_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-hbase_${VERSION}-bin.tar.gz
    $ cd geomesa-hbase_${VERSION}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)

.. _hbase_install_source:

Building from Source
--------------------

GeoMesa HBase may also be built from source. For more information, refer to the instructions on
`GitHub <https://github.com/locationtech/geomesa#building-from-source>`__.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa HBase
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-hbase/geomesa-hbase-dist``.

.. _hbase_deploy_distributed_runtime:

Installing the GeoMesa Distributed Runtime JAR
----------------------------------------------

GeoMesa uses custom HBase filters and coprocessors to speed up queries.

You must deploy the distributed runtime jar to the directory specified by the HBase configuration variable
``hbase.dynamic.jars.dir``.  This is set to ``${hbase.rootdir}/lib`` by default. Copy the distribute runtime jar to
this directory as follows:

.. code-block:: bash

<<<<<<< HEAD
  $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
  .. group-tab:: HBase 2.x

    .. code-block:: bash

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)

  .. group-tab:: HBase 1.x

    .. code-block:: bash

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 759250a5978 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> c94191b4100 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 82e909b2706 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 10515a5e00b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> c8b35a9b6a7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67bcedea2bd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8b0a7aef724 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0c0a77b3b4f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4a915016644 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 94403e4031a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 9943aadb6e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fc9b565ea78 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 561d6db7196 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 33511dd1c3b (Merge branch 'feature/postgis-fixes')
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f491c29f0ce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 3d2884e6918 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a3f6048ea95 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6b807d2b8bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> c4c480adf3d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c05c303bf4b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 87ccedac7ab (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 02033dbfe43 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07fc2c6617c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7b9d0528733 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 93fd90bb0f1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bc4724d3c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 30bfb17e445 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 10be5d2340b (Merge branch 'feature/postgis-fixes')
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
=======
      hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d738e789b00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> e20773dbffd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
      $ hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar ${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d8597d50211 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0fe31dc91ca (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 6ca2cca6595 (GEOMESA-3176 Docs - fix download links in install instructions)

If running on top of Amazon S3, you will need to use the ``aws s3`` command line tool.

.. code-block:: bash

<<<<<<< HEAD
  $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
  .. group-tab:: HBase 2.x

    .. code-block:: bash

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase2_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)

  .. group-tab:: HBase 1.x

    .. code-block:: bash

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 759250a5978 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> c94191b4100 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 82e909b2706 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 10515a5e00b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> c8b35a9b6a7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67bcedea2bd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8b0a7aef724 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0c0a77b3b4f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4a915016644 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 94403e4031a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 9943aadb6e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fc9b565ea78 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 561d6db7196 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 33511dd1c3b (Merge branch 'feature/postgis-fixes')
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f491c29f0ce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 3d2884e6918 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a3f6048ea95 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6b807d2b8bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> c4c480adf3d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c05c303bf4b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 87ccedac7ab (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 02033dbfe43 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07fc2c6617c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7b9d0528733 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 93fd90bb0f1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bc4724d3c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 30bfb17e445 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 10be5d2340b (Merge branch 'feature/postgis-fixes')
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
=======
      aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d738e789b00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> e20773dbffd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
      $ aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-hbase1_${VERSION}.jar s3://${hbase.dynamic.jars.dir}/
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d8597d50211 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0fe31dc91ca (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 6ca2cca6595 (GEOMESA-3176 Docs - fix download links in install instructions)

If required, you may disable distributed processing by setting the system property ``geomesa.hbase.remote.filtering``
to ``false``. Note that this may have an adverse effect on performance.

.. _registering_coprocessors:

Register the Coprocessors
^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming that you have installed the distributed runtime JAR under ``hbase.dynamic.jars.dir``, coprocessors will be
registered automatically when you call ``createSchema`` on a data store. Alternatively, the coprocessors may be
registered manually. See :ref:`coprocessor_alternate` for details.

For more information on managing coprocessors see
`Coprocessor Introduction <https://blogs.apache.org/hbase/entry/coprocessor_introduction>`_ on Apache's Blog.

.. _setting_up_hbase_commandline:

Setting up the HBase Command Line Tools
---------------------------------------

.. warning::

    To use the HBase data store with the command line tools, you need to install
    the distributed runtime first. See :ref:`hbase_deploy_distributed_runtime`.

GeoMesa comes with a set of command line tools for managing HBase features located in
``geomesa-hbase_${VERSION}/bin/`` of the binary distribution.

GeoMesa requires ``java`` to be available on the default path.

Configuring the Classpath
^^^^^^^^^^^^^^^^^^^^^^^^^

GeoMesa needs HBase and Hadoop JARs on the classpath. These are not bundled by default, as they should match
the versions installed on the target system.

If the environment variables ``HBASE_HOME`` and ``HADOOP_HOME`` are set, then GeoMesa will load the appropriate
JARs and configuration files from those locations and no further configuration is required. Otherwise, you will
be prompted to download the appropriate JARs the first time you invoke the tools. Environment variables can be
specified in ``conf/*-env.sh`` and dependency versions can be specified in ``conf/dependencies.sh``.

For advanced scenarios, the environmental variables ``GEOMESA_HADOOP_CLASSPATH`` and ``GEOMESA_HBASE_CLASSPATH``
can be set to override all other logic.

If no environment variables are set but the ``hbase`` and ``hadoop`` commands are available, then
GeoMesa will interrogate them for their classpaths by running the ``hadoop classpath`` and ``hbase classpath``
commands. Note that this can be slow, so it is usually better to use ``GEOMESA_HADOOP_CLASSPATH`` and
``GEOMESA_HBASE_CLASSPATH`` as described above.

.. tabs::

    .. group-tab:: Standard

        Configure GeoMesa to use pre-installed HBase and Hadoop distributions:

        .. code-block:: bash

            $ export HADOOP_HOME=/path/to/hadoop
            $ export HBASE_HOME=/path/to/hbase
            $ export GEOMESA_HBASE_HOME=/opt/geomesa
            $ export PATH="${PATH}:${GEOMESA_HBASE_HOME}/bin"

    .. group-tab:: Amazon EMR

        When using EMR to install HBase or Hadoop there are AWS specific jars that need to be used (e.g. EMR FS).
        It is recommended to use EMR to install Hadoop and/or HBase in order to properly configure and install these
        dependencies (especially when using HBase on S3).

        If you used EMR to install Hadoop and HBase, you can view their classpaths using the ``hadoop classpath`` and
        ``hbase classpath`` commands to build an appropriate classpath to include jars and configuration files for
        GeoMesa HBase:

        .. code-block:: bash

            $ export GEOMESA_HADOOP_CLASSPATH=$(hadoop classpath)
            $ export GEOMESA_HBASE_CLASSPATH=$(hbase classpath)
            $ export GEOMESA_HBASE_HOME=/opt/geomesa
            $ export PATH="${PATH}:${GEOMESA_HBASE_HOME}/bin"

    .. group-tab:: HDP

        Configure the environment to use an HDP install:

        .. code-block:: bash

            $ export HADOOP_HOME=/usr/hdp/current/hadoop-client/
            $ export HBASE_HOME=/usr/hdp/current/hbase-client/
            $ export GEOMESA_HBASE_HOME=/opt/geomesa
            $ export PATH="${PATH}:${GEOMESA_HBASE_HOME}/bin"

    .. group-tab:: Standalone

        If there is no local HBase instance, the necessary JARs can be installed by downloading them. Modify the
        version numbers in ``geomesa-hbase_${VERSION}/conf/dependencies.sh`` to match the target system and use
        ``geomesa-hbase_${VERSION}/bin/install-dependencies.sh`` to install them:

        .. code-block:: bash

            $ cd geomesa-hbase_${VERSION}/bin
            $ ./install-dependencies.sh

        In order to connect to a cluster, an appropriate ``hbase-site.xml`` is required. Copy it from your cluster
        into ``geomesa-hbase_${VERSION}/conf/``.

        In order to run map/reduce jobs, copy the Hadoop ``*-site.xml`` configuration files
        from your Hadoop installation into ``geomesa-hbase_${VERSION}/conf``.

In order to run map/reduce and Spark jobs, you will need to put ``hbase-site.xml`` into a JAR on the distributed
classpath. Add it at the root level of the ``geomesa-hbase-datastore`` JAR in the ``lib`` folder:

.. code-block:: bash

    $ zip -r lib/geomesa-hbase-datastore_${VERSION}.jar hbase-site.xml

.. warning::

    Ensure that the ``hbase-site.xml`` is at the root (top) level of your JAR, otherwise it will not be picked up.

GeoMesa also provides the ability to add additional JARs to the classpath using the environmental variable
``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
classpath, giving it highest precedence in the classpath. Users can provide directories of jar files or individual
files using a colon (``:``) as a delimiter. These entries will also be added the the map-reduce libjars variable.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following command:

.. code-block:: bash

    $ ./bin/install-shapefile-support.sh

For logging, see :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Use the ``geomesa-hbase classpath`` command to print the final classpath that will be used when executing GeoMesa
commands.

Configuring the Path
^^^^^^^^^^^^^^^^^^^^

In order to be able to run the ``geomesa-hbase`` command from anywhere, you can set the environment
variable ``GEOMESA_HBASE_HOME`` and add it to your path by modifying your bashrc file:

.. code-block:: bash

    $ echo 'export GEOMESA_HBASE_HOME=/path/to/geomesa-hbase_${VERSION}' >> ~/.bashrc
    $ echo 'export PATH=${GEOMESA_HBASE_HOME}/bin:$PATH' >> ~/.bashrc
    $ source ~/.bashrc
    $ which geomesa-hbae
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
    /path/to/geomesa-hbase_${VERSION}/bin/geomesa-hbase
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)

Running Commands
^^^^^^^^^^^^^^^^

Test the command that invokes the GeoMesa Tools:

.. code-block:: bash

    $ geomesa-hbase

The output should look like this::

    Usage: geomesa-hbase [command] [command options]
      Commands:
      ...

For details on the available commands, see :ref:`hbase_tools`.

.. _install_hbase_geoserver:

Installing GeoMesa HBase in GeoServer
-------------------------------------

.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

The HBase GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``$GEOMESA_HBASE_HOME/dist/gs-plugins/geomesa-hbase-gs-plugin_${VERSION}-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory.

This distribution does not include the HBase client, Hadoop or Zookeeper JARs. These JARs can be installed
using the ``bin/install-dependencies.sh`` script included in the binary distribution. Before
running, set the version numbers in ``conf/dependencies.sh`` to match your target installation as needed.

The HBase data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist).

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
The specific JARs needed for some common configurations are listed below:

.. tabs::

    .. tab:: HBase 2.2

        * commons-cli-1.2.jar
        * commons-configuration-1.6.jar
        * commons-io-2.5.jar
        * commons-logging-1.1.3.jar
        * hadoop-auth-2.8.5.jar
        * hadoop-common-2.8.5.jar
        * hadoop-hdfs-2.8.5.jar
        * hadoop-hdfs-client-2.8.5.jar
        * hadoop-mapreduce-client-core-2.8.5.jar
        * hbase-client-2.2.3.jar
        * hbase-common-2.2.3.jar
        * hbase-hadoop-compat-2.2.3.jar
        * hbase-mapreduce-2.2.3.jar
        * hbase-protocol-2.2.3.jar
        * hbase-protocol-shaded-2.2.3.jar
        * hbase-shaded-miscellaneous-2.2.1.jar
        * hbase-shaded-netty-2.2.1.jar
        * hbase-shaded-protobuf-2.2.1.jar
        * htrace-core4-4.1.0-incubating.jar
        * metrics-core-2.2.0.jar
        * metrics-core-3.2.6.jar
        * netty-3.6.2.Final.jar
        * netty-all-4.1.48.Final.jar
        * protobuf-java-2.5.0.jar
        * zookeeper-3.4.14.jar

    .. tab:: HBase 1.4

        * commons-cli-1.2.jar
        * commons-configuration-1.6.jar
        * commons-io-2.5.jar
        * commons-logging-1.1.3.jar
        * hadoop-auth-2.8.5.jar
        * hadoop-common-2.8.5.jar
        * hadoop-hdfs-2.8.5.jar
        * hadoop-hdfs-client-2.8.5.jar
        * hadoop-mapreduce-client-core-2.8.5.jar
        * hbase-client-1.4.13.jar
        * hbase-common-1.4.13.jar
        * hbase-hadoop-compat-1.4.13.jar
        * hbase-protocol-1.4.13.jar
        * htrace-core-3.1.0-incubating.jar
        * htrace-core4-4.1.0-incubating.jar
        * metrics-core-2.2.0.jar
        * netty-3.6.2.Final.jar
        * netty-all-4.1.48.Final.jar
        * protobuf-java-2.5.0.jar
        * zookeeper-3.4.14.jar

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
Restart GeoServer after the JARs are installed.

Connecting to External HBase Clusters Backed By S3
--------------------------------------------------

To use a EMR cluster to connect to an existing, external HBase Cluster first follow the above instructions to setup the
new cluster and install GeoMesa.

The next step is to obtain the ``hbase-site.xml`` for the external HBase Cluster, copy to the new EMR cluster and
copy it into ``${GEOMESA_HBASE_HOME}/conf``. At this point you may run the ``geomesa-hbase`` command line tools.
In order to run Spark or Map/Reduce jobs, ensure that ``hbase-site.xml`` is zipped into a JAR, as described above.

Configuring HBase on Azure HDInsight
------------------------------------

HDInsight generally creates ``HBASE_HOME`` in HDFS under the path ``/hbase``. In order to make the GeoMesa
coprocessors and filters available to the region servers, use the ``hadoop`` filesystem command to put
the GeoMesa JAR there:

.. code-block:: shell

    hadoop fs -mkdir /hbase/lib
    hadoop fs -put geomesa-hbase-distributed-runtime-hbase2-$VERSION.jar /hbase/lib/
