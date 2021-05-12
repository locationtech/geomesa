FileSystem Data Store Example
=============================

In this simple example we will ingest a small CSV into a local filesystem data store partitioned by an daily,z2-2bit
scheme. To begin, start by untaring the geomesa-fs distribution. Inside this distribution you will find an examples
folder which contains an example csv file that we will ingest. First set the version you want to use:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 419e2b9752 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 86bc039842 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e007dde981 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7b2b25cc3c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8dec77199d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1f19a648d1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 308738bcef (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 983d0b0983 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 419e2b9752 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1f19a648d1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d13d2eab26 (GEOMESA-3176 Docs - fix download links in install instructions)

.. parsed-literal::

    $ export TAG="|release_version|"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 17c1f8a18c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8b8075f529 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64e2e43818 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 610d4a0e13 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d25af58f86 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 3b4f3da2e1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 17c1f8a18c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 64e2e43818 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
    $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version
=======
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 17c1f8a18c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b4f3da2e1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 17c1f8a18c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)

Then download and extract the binary distribution:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1f19a648d1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

<<<<<<< HEAD
Then download and extract the binary distribution:
<<<<<<< HEAD
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 235bc2820d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 93fa8c797f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7813a31aa (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 71c56e6b77 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 703374130f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2aa923cc9 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 235bc2820d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)

.. parsed-literal::

    $ export TAG="|release_version|"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
    $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version
=======
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)

Then download and extract the binary distribution:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 10ade3dcd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')

.. parsed-literal::

<<<<<<< HEAD
  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)

.. parsed-literal::

=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
    $ export TAG="|release_version|"
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 31ac28fff4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 26bf3ffae4 (GEOMESA-3176 Docs - fix download links in install instructions)
    $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version

Then download and extract the binary distribution:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')

<<<<<<< HEAD
=======
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3d5144418e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 93fa8c797f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)

.. parsed-literal::

    $ export TAG="|release_version|"
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:
>>>>>>> d13d2eab26 (GEOMESA-3176 Docs - fix download links in install instructions)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
.. parsed-literal::

    $ export TAG="|release_version|"
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:
>>>>>>> 308738bcef (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 1f19a648d1 (GEOMESA-3176 Docs - fix download links in install instructions)

Then download and extract the binary distribution:
<<<<<<< HEAD
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 235bc2820d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 93fa8c797f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7813a31aa (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 71c56e6b77 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 703374130f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2aa923cc9 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 235bc2820d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)

.. parsed-literal::

    $ export TAG="|release_version|"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
    $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version
=======
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)

Then download and extract the binary distribution:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 983d0b0983 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 10ade3dcd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')

.. parsed-literal::
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
.. parsed-literal::

    $ export TAG="|release_version|"
<<<<<<< HEAD
<<<<<<< HEAD
    $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version
=======
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)

Then download and extract the binary distribution:
>>>>>>> 308738bcef (GEOMESA-3176 Docs - fix download links in install instructions)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')

=======
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c7403e8a5f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4ac5af223d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 62a28bf2e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 93fa8c797f (GEOMESA-3176 Docs - fix download links in install instructions)

>>>>>>> 8dec77199d (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bd3233180f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 703374130f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8dec77199d (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1f99f4bcb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 338d952d43 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 30cd5ab927 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ffe4f9fd41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ecddd8c3e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 036a470d76 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ecd45f2c3e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6fc8851879 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bd3233180f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6514df9383 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c1f99f4bcb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9c24ca3d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. note::
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e007dde981 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 7b2b25cc3c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)

.. parsed-literal::
>>>>>>> 86bc039842 (GEOMESA-3176 Docs - fix download links in install instructions)

<<<<<<< HEAD
  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 6fc8851879 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6514df9383 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c9c24ca3d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 1d03325c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6fc8851879 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6514df9383 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7813a31aa (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2aa923cc9 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
    $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version
<<<<<<< HEAD
<<<<<<< HEAD
=======
.. parsed-literal::
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')

<<<<<<< HEAD
=======
=======
.. parsed-literal::
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')

>>>>>>> 8dec77199d (GEOMESA-3176 Docs - fix download links in install instructions)
    $ export TAG="|release_version|"
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:
>>>>>>> 10ade3dcd (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
Then download and extract the binary distribution:
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 8dec77199d (GEOMESA-3176 Docs - fix download links in install instructions)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
Then download and extract the binary distribution:
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)

<<<<<<< HEAD
.. parsed-literal::

    $ export TAG="|release_version|"
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:
=======
<<<<<<< HEAD
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

=======
<<<<<<< HEAD
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
.. note::
=======
>>>>>>> 2aa923cc9 (GEOMESA-3176 Docs - fix download links in install instructions)

.. parsed-literal::
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)

    $ export TAG="|release_version|"
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:

<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6fc8851879 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======

>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3995e6a61c (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6514df9383 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======

>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f9e8439b0 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> bd3233180f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======

>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bc73cc4d41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98eacc8492 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 664350d86f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
.. parsed-literal::

=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
    $ export TAG="|release_version|"
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:
<<<<<<< HEAD
>>>>>>> 419e2b9752 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
>>>>>>> 86bc039842 (GEOMESA-3176 Docs - fix download links in install instructions)

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
>>>>>>> 98eacc8492 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 664350d86f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 279de44d3 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> b7c6b904be (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a737b13ed (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4b70e7f8f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 664350d86f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b42125fac1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a12ea194c (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5c3f315bc0 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======

>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> c1f99f4bcb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b8075f529 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 338d952d43 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64e2e43818 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======

>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> 30cd5ab927 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 610d4a0e13 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> ffe4f9fd41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d25af58f86 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> ecddd8c3e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9c39003417 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 036a470d76 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> ad7067b815 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e3976d40bc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7dfac253b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 269558cf9e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3b4f3da2e1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ad7067b815 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17c1f8a18c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e3976d40bc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 82d93995e4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7dfac253b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 760781f8dd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> a0befba4ce (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f7f4f4a836 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bcf78618df (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 31f6953312 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> df644ff83d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 31f6953312 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 7813a31aa (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6150564577 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 449fb2085b (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc622ff59f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
.. note::
=======
=======
>>>>>>> a1f15598cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. parsed-literal::
>>>>>>> e007dde981 (GEOMESA-3176 Docs - fix download links in install instructions)

    $ export TAG="|release_version|"
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:
=======
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

<<<<<<< HEAD
>>>>>>> 1bb69524c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 760781f8dd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======

>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f7f4f4a836 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> a0befba4ce (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======

>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> df644ff83d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> bcf78618df (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======

>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 31f6953312 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
.. note::
=======
>>>>>>> 2aa923cc9 (GEOMESA-3176 Docs - fix download links in install instructions)

.. parsed-literal::
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)

    $ export TAG="|release_version|"
    # note: |scala_binary_version| is the Scala build version
    $ export VERSION="|scala_binary_version|-${TAG}"

Then download and extract the binary distribution:

>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a1f15598cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> ecd45f2c3e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6fc8851879 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 3995e6a61c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6514df9383 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bd3233180f (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> bc73cc4d41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 279de44d3 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> b7c6b904be (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 813f24c01d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 98eacc8492 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b42125fac1 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 1a737b13ed (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 664350d86f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5c3f315bc0 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c9c24ca3d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8b8075f529 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 64e2e43818 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> 3d5144418e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> c7403e8a5f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 31ac28fff4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 4ac5af223d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 26bf3ffae4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

>>>>>>> 62a28bf2e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-fs_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-fs_${VERSION}-bin.tar.gz
    $ cd geomesa-fs_${VERSION}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    $ cat examples/csv/example.csv
=======
=======
>>>>>>> c94191b4100 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 82e909b2706 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10515a5e00b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8b35a9b6a7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 67bcedea2bd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8b0a7aef724 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c0a77b3b4f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4a915016644 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 94403e4031a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943aadb6e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> fc9b565ea78 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 561d6db7196 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 33511dd1c3b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f491c29f0ce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3d2884e6918 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a3f6048ea95 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6b807d2b8bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c4c480adf3d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c05c303bf4b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 87ccedac7ab (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 02033dbfe43 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07fc2c6617c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7b9d0528733 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 93fd90bb0f1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bc4724d3c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30bfb17e445 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10be5d2340b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d738e789b00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e20773dbffd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d8597d50211 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 6f731317e48 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 82e507ba4a9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4a73fdef2c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4acea2715bd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e06dfcbaf8b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9004d2b9d79 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 1a841faf4a6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> aa8f71ca922 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> aaac9bf1b27 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b1f6c1af54c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 91922599417 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 519b3ef86bd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8d05a3df150 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fcf04b94800 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ebd7747a36e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 82729e78d72 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2cffd2e6cb1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> be6a86bb185 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5d401a5f03a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3ad4b798923 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f366b024984 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 646bc4ad1bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 32a3a53f7b2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4fb80fcb452 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f26d218941d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 49b8e3436d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 228b43516c6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> acd35427281 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5e90ae817e6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c4c06b015d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8b6a17d4602 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ca09aee9ddb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 60e9a6fe7fe (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a6d6ade82 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28b8e420a70 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8b242672b1d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3e6c36184cd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1265f02dde7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c5932657944 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a2ef6f2ce81 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7e850561b05 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b1821de04 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4310da4eb88 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e9dec044e68 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 649479f8be7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 304b536e747 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7d2b7915553 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6cdb8338fae (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e20a90450b8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fbebb1834aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8dd024f6791 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 41265694526 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f551eb3a070 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d5fa0ac1eaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 3b8bc3492ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fcb61d68c50 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ba70f3baa63 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6882b2b9465 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b8b43d1fb39 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72cd5cd0cf3 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c58dd3314eb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> aff2e861be0 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 64a645069b5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e175db3a7a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e61482deb43 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f3b985c5f32 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 02b8f16a86b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 37ed3a85a57 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5cc989d3914 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a0d869facaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> df2efe122d4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ba993d6d91e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e2cb1c93c8e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 873748b2cc6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 1c1f4502b6c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 169e89961d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 29b01d02901 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4ad750f4fb0 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2075dd77a4a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> b300513d9ad (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 19b13725838 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f59d2a8b815 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 23d4578f247 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 20b9743bc81 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30d8211aa7c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5be258071c7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2f93fe7cb8d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a1a882724a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a1e55cfea93 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a8a55c9ccc4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 709d36fab5e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> aaeb070ee87 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5376733e6c7 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f0809ab5b94 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8c308e0e77 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f07bf95f4d9 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 04c2f558543 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51900547c20 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1f87fe876dd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> bbbadb24066 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 20c5f3e576b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a498a301430 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 21911db1200 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d6895f2a469 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ae9ccc22c0e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 277a08c801c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7bf393ad279 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 221b2c644b7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> daf391a2cde (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 355367142d8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 56b67bddd45 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 467e112456c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d37b48dc9b5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> fa0314d0e82 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 58898bfe578 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 601c91f36c1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e2ba4c1ccbb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7ef5fcc896b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ab46ac2ae83 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56fef72fa0c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8b1b7e6435b (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5ed2a38fa04 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 395d692be09 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 558a578357f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e7d80a38fc2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f5c5bbdbe9a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 79b43c4cc74 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ec2cc1064bf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5fb5365f268 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b96a08f91e5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f29cf610dd0 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e5025c53d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b2c5cb17bee (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8b30ff6e9ce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d434bbd6d35 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> bef8517af6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6e033924bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 64b7e20569b (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 32573c77d12 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 03074988ee2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70a8367cb8c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ba972d8430f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 77bd7994691 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 41da9a3feea (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 9ce9ec871df (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 587a433dd65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 862aae30a3b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1411b134077 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> edbb31e3b57 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> cdad6fdd88b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1ef7c8d088c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 59df63fdc54 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
=======
=======
>>>>>>> aa7d14ebd9e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 52534044679 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 938e1e9e303 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e295de8711f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0d2744cf947 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2c2d7286be0 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e4d9e938750 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 190b2701741 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 373ca944cd4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ab621f41860 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 5f216af33ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ebdc229ea6e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 35aa8f85a2f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 115a7655e1f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70261f0d510 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 81399d5525d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7b29e263f34 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 077885d6ffe (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f110463a26 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 86ec77f9cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1bcc513539 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 338d952d43 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 64e2e43818 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 1bcc513539 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f110463a26 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 86ec77f9cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8b8075f529 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> daac00ce2ae (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ed80d711a6b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 92674ff9d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc622ff59f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9a4dff9e4f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fc6650ecf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1bb69524c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e007dde981 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 66c2e4df7e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8c9ca1211a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 760781f8dd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f7f4f4a836 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a22efb4d66 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a0befba4ce (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> df644ff83d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cad375176 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bcf78618df (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 31f6953312 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1e39c0c27d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a1f15598cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7b2b25cc3c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 1d03325c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 963feec660 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ecd45f2c3e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6fc8851879 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 84ac051f28 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3995e6a61c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6514df9383 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe3de10f11 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d0785cf307 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8dec77199d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> bc73cc4d41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5c6b9f1f8b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 813f24c01d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 98eacc8492 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d44ff473d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1a737b13ed (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 664350d86f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a35efac12e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fd5565dfba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1f19a648d1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c9c24ca3d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 2654d9c120 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 23c0241798 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4b70e7f8fb (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 85663f71c4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 71c56e6b77 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e27137ff78 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> dddc42e265 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d969b4c2d5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4564a1634e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f73ef99f1e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> be05354746 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 235bc2820d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4a05f27ba4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 256a642cc1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> aceaf3bd81 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7efa8bf5c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 93c8cf1b3a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 94efcf84c2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8929cbff5b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 93fa8c797f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c82dea9d29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2142d07ded (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a398ab1183 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a8d6366d72 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6f30a087ee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b041fcc3eb (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 3acb47e615 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bd3233180f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 703374130f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b42125fac1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5c3f315bc0 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4b70e7f8f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> b7c6b904be (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b0 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 71c56e6b77 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e27137ff78 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> dddc42e265 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d969b4c2d5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4564a1634e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f73ef99f1e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> be05354746 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 235bc2820d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4a05f27ba4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 256a642cc1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> aceaf3bd81 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7efa8bf5c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 93c8cf1b3a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 94efcf84c2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8929cbff5b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 93fa8c797f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c82dea9d29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2142d07ded (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a398ab1183 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a8d6366d72 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6f30a087ee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b041fcc3eb (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 3acb47e615 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bd3233180f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 703374130f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b42125fac1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5c3f315bc0 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4b70e7f8f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> b7c6b904be (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b0 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cfa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> 449fb2085b (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 23c0241798 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 92674ff9d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4b70e7f8fb (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> bc622ff59f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9a4dff9e4f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 6ca2cca6595 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0fe31dc91ca (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 983d0b0983 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 30cd5ab927 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 610d4a0e13 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ffe4f9fd41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d25af58f86 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ecddd8c3e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9c39003417 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 036a470d76 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 419e2b9752 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 269558cf9e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3b4f3da2e1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ad7067b815 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 694bcd3776 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17c1f8a18c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e3976d40bc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6cd1151639 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 82d93995e4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7dfac253b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1973f72e77 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 86bc039842 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6150564577 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5dcf23da52 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d13d2eab26 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c7403e8a5f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7bdbfa5524 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 31ac28fff4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4ac5af223d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7bff4d0018 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 26bf3ffae4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 62a28bf2e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30cd5ab927 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c7403e8a5f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7bdbfa5524 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
=======
>>>>>>> 610d4a0e13 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 31ac28fff4 (GEOMESA-3176 Docs - fix download links in install instructions)

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
>>>>>>> 449fb2085b (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 760781f8dd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 813f24c01d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
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
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2142d07ded (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 760781f8dd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2142d07ded (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 813f24c01d (GEOMESA-3176 Docs - fix download links in install instructions)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 760781f8dd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ecd45f2c3e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8b8075f529 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
=======
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 449fb2085b (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
=======

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

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> bc622ff59f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9a4dff9e4f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

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
=======
>>>>>>> bc622ff59f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 4b70e7f8fb (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 85663f71c4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d4c5768cfa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 85663f71c4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d4c5768cfa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc622ff59f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c0241798 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 85663f71c4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
<<<<<<< HEAD
>>>>>>> 256a642cc1 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 279de44d3 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 85663f71c4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

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
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9a4dff9e4f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c0241798 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 449fb2085b (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 9a4dff9e4f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 256a642cc1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2142d07ded (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b7c6b904be (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 9a4dff9e4f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92674ff9d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4c5768cfa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 92674ff9d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cfa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c1f99f4bcb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 4b70e7f8fb (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 85663f71c4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d4c5768cfa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c0241798 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 85663f71c4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 256a642cc1 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 279de44d3 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c0241798 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 256a642cc1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2142d07ded (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b7c6b904be (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cfa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c1f99f4bcb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8c9ca1211a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> c9c24ca3d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
<<<<<<< HEAD
>>>>>>> 64e2e43818 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0fc6650ecf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1e39c0c27d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d0785cf307 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd5565dfba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fd5565dfba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
>>>>>>> 1e39c0c27d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d0785cf307 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fd5565dfba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1bb69524c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a1f15598cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c0241798 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bc622ff59f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 4b70e7f8fb (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 85663f71c4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a4dff9e4f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cfa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7813a31aa (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 71c56e6b77 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c1f99f4bcb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 338d952d43 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c9c24ca3d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c9c24ca3d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 86ec77f9cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e27137ff78 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 338d952d43 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 338d952d43 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
<<<<<<< HEAD
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> 2654d9c120 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2654d9c120 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 86ec77f9cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 86ec77f9cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f110463a26 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> dddc42e265 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f110463a26 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f110463a26 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> d969b4c2d5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4564a1634e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f73ef99f1e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> be05354746 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1f15598cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> fe3de10f11 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 86ec77f9cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

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
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 2aa923cc9 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 235bc2820d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b0 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fe3de10f11 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4a05f27ba4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> fe3de10f11 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> f9e8439b0 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe3de10f11 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 256a642cc1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> aceaf3bd81 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 7efa8bf5c8 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 93c8cf1b3a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f9e8439b0 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> bd3233180f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 94efcf84c2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8929cbff5b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a35efac12e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1bcc513539 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

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
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 10ade3dcd (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 703374130f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 93fa8c797f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> 4b70e7f8f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a12ea194c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a35efac12e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c82dea9d29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> a35efac12e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> a12ea194c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a35efac12e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 279de44d3 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> b7c6b904be (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 2142d07ded (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a398ab1183 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 4b70e7f8f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> b42125fac1 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> a8d6366d72 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6f30a087ee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a12ea194c (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 5c3f315bc0 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> b041fcc3eb (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 3acb47e615 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1f19a648d1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 308738bcef (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1f99f4bcb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8b8075f529 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 338d952d43 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 64e2e43818 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9627a2de6a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b046434188 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
=======

>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 983d0b0983 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 30cd5ab927 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 610d4a0e13 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> ecddd8c3e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9c39003417 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 036a470d76 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 269558cf9e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3b4f3da2e1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> ad7067b815 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 17c1f8a18c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> e3976d40bc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 82d93995e4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7dfac253b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 62a28bf2e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
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
=======
<<<<<<< HEAD
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 4ac5af223d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

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
<<<<<<< HEAD
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> ffe4f9fd41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
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

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> d25af58f86 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> ecddd8c3e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 9c39003417 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 036a470d76 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 419e2b9752 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 269558cf9e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 3b4f3da2e1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> ad7067b815 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 694bcd3776 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 17c1f8a18c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e3976d40bc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6cd1151639 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 82d93995e4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72bafaf54b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7dfac253b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1973f72e77 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 10ade3dcd1 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 86bc039842 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6150564577 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 5dcf23da52 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 279de44d3f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 449fb2085b (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c0241798 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d1506029d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 92674ff9d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc622ff59f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 7ec7e43c91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9a4dff9e4f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a12ea194c8 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 1cc4dadf2e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 2586fa6a40 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0fc6650ecf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1bb69524c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7813a31aa (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 71c56e6b77 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> e007dde981 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 62a28bf2e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e27137ff78 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 66c2e4df7e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8c9ca1211a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
<<<<<<< HEAD
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 6a26990f7 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0b36b46b5f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 760781f8dd (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> dddc42e265 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f7f4f4a836 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a22efb4d66 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> baa90e85a (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> d969b4c2d5 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> a0befba4ce (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4564a1634e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> df644ff83d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1cad375176 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 46c05fed5 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> f73ef99f1e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> bcf78618df (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> be05354746 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 31f6953312 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 1e39c0c27d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7b2b25cc3c (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a1f15598cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
>>>>>>> 1d03325c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6fc8851879 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3995e6a61c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6514df9383 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2aa923cc9 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> bdcaf0ab5e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 235bc2820d (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 7b2b25cc3c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
=======
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4a05f27ba4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1d03325c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 963feec660 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 8cbe155de (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 8150bc0e65 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 256a642cc1 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> ecd45f2c3e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> aceaf3bd81 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6fc8851879 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 84ac051f28 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 4d9f87a514 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 7efa8bf5c8 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 3995e6a61c (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe3de10f11 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 93c8cf1b3a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6514df9383 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fe3de10f11 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f9e8439b0 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> bd3233180f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 94efcf84c2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> cabde54511 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d0785cf307 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8929cbff5b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c91f823574 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d0785cf307 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8dec77199d (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
>>>>>>> bc73cc4d41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98eacc8492 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 1a737b13ed (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 664350d86f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 10ade3dcd (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 703374130f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 93fa8c797f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 8dec77199d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
=======
>>>>>>> 4b70e7f8f (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c82dea9d29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> bc73cc4d41 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 5c6b9f1f8b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 279de44d3 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> b7c6b904be (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 2142d07ded (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 813f24c01d (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a398ab1183 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 98eacc8492 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9d44ff473d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 4b70e7f8f (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> b42125fac1 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> a8d6366d72 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 1a737b13ed (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a35efac12e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6f30a087ee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 664350d86f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a35efac12e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a12ea194c (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 5c3f315bc0 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> b041fcc3eb (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 3e34d2451e (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fd5565dfba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 3acb47e615 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a85aff60b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fd5565dfba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1f19a648d1 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> c1f99f4bcb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9c24ca3d7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2654d9c120 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4492950388 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> b9c0fc77b9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f110463a26 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> aff2787fc2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> d69216810e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 86ec77f9cb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0753bfcbe4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 76908c410a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 17f16286aa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 1bcc513539 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======

>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3d5144418e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d13d2eab26 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> c7403e8a5f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7bdbfa5524 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 31ac28fff4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 62a28bf2e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4ac5af223d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7bff4d0018 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======

>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 26bf3ffae4 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 62a28bf2e7 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    $ cat examples/ingest/csv/example.csv
>>>>>>> 759250a5978 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

The output should look like::

    ID,Name,Age,LastSeen,Friends,Lon,Lat,Vis
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan","patronus->10,expelliarmus->9",-100.236523,23,user
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry","accio->10",40.232,-53.2356,user
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort","potions->10",3,-62.23,user&admin

As you can see, there are 3 records in the file. GeoMesa ships with a pre-installed SimpleFeatureType and converter
for this example file which can be found in the ``conf/application.conf`` file. Running ``bin/geomesa-fs env`` will
show that there is an ``example-csv`` type and converter installed along with many other types including twitter, gdelt,
osm, etc:

.. code-block:: bash

    $ bin/geomesa-fs env

The output should look like::

    Simple Feature Types:
    example-avro
    example-csv
    example-csv-complex
    ...

    Simple Feature Type Converters:
    example-avro-header
    example-avro-no-header
    example-csv
    ...

For this example we'll ingest the three rows to a local filesystem. Note that the records are all on different days::

    2015-05-06
    2015-06-07
    2015-10-23

and that the geometries fall into two quadrants of the world. The first record is in the upper left quadrant and the
second and third records are in the lower right quadrant::

    Lon          Lat
    -100.236523  23
    40.232      -53.2356
    3           -62.23

Now lets ingest.

.. code-block:: bash

    $ bin/geomesa-fs ingest -p /tmp/dstest -e parquet -s example-csv -C example-csv \
    --partition-scheme daily,z2-2bit examples/csv/example.csv

The output should look like::

    INFO  Creating schema example-csv
    INFO  Running ingestion in local mode
    INFO  Ingesting 1 file with 1 thread
    [============================================================] 100% complete 3 ingested 0 failed in 00:00:01
    INFO  Local ingestion complete in 00:00:01
    INFO  Ingested 3 features with no failures.

We can verify our ingest by running an export:

.. code-block:: bash

    $ bin/geomesa-fs export -p /tmp/dstest -f example-csv

The output should look like::

    id,fid:Integer:index=false,name:String:index=true,age:Integer:index=false,lastseen:Date:default=true:index=false,*geom:Point:srid=4326
    26236,26236,Hermione,25,2015-06-07T00:00:00.000Z,POINT (40.232 -53.2356)
    3233,3233,Severus,30,2015-10-23T00:00:00.000Z,POINT (3 -62.23)
    23623,23623,Harry,20,2015-05-06T00:00:00.000Z,POINT (-100.236523 23)
    INFO  Feature export complete to standard out in 1676ms for 3 features

Now lets inspect the filesystem:

.. code-block:: bash

    $ find /tmp/dstest | sort

The output should look like::

    /tmp/dstest/
    /tmp/dstest/example-csv
    /tmp/dstest/example-csv/2015
    /tmp/dstest/example-csv/2015/05
    /tmp/dstest/example-csv/2015/05/06
    /tmp/dstest/example-csv/2015/05/06/2_Wcec6a2ec594a4a2eb7c7980a1baf4ab3.parquet
    /tmp/dstest/example-csv/2015/05/06/.2_Wcec6a2ec594a4a2eb7c7980a1baf4ab3.parquet.crc
    /tmp/dstest/example-csv/2015/06
    /tmp/dstest/example-csv/2015/06/07
    /tmp/dstest/example-csv/2015/06/07/1_Wcc082b9cf9bc4965b4cbf64741fee5b6.parquet
    /tmp/dstest/example-csv/2015/06/07/.1_Wcc082b9cf9bc4965b4cbf64741fee5b6.parquet.crc
    /tmp/dstest/example-csv/2015/10
    /tmp/dstest/example-csv/2015/10/23
    /tmp/dstest/example-csv/2015/10/23/1_W741f2151a4ed4eec97461a174a8588b7.parquet
    /tmp/dstest/example-csv/2015/10/23/.1_W741f2151a4ed4eec97461a174a8588b7.parquet.crc
    /tmp/dstest/example-csv/metadata
    /tmp/dstest/example-csv/metadata/storage.json
    /tmp/dstest/example-csv/metadata/.storage.json.crc
    /tmp/dstest/example-csv/metadata/update-2015-05-06-2-12240906-4171-4ab0-acfe-d2ce9c5fff76.json
    /tmp/dstest/example-csv/metadata/.update-2015-05-06-2-12240906-4171-4ab0-acfe-d2ce9c5fff76.json.crc
    /tmp/dstest/example-csv/metadata/update-2015-06-07-1-ecd68700-88e3-4f04-9438-84b6ab935907.json
    /tmp/dstest/example-csv/metadata/.update-2015-06-07-1-ecd68700-88e3-4f04-9438-84b6ab935907.json.crc
    /tmp/dstest/example-csv/metadata/update-2015-10-23-1-667f27a7-4f64-472a-80ed-82e8f1e65575.json
    /tmp/dstest/example-csv/metadata/.update-2015-10-23-1-667f27a7-4f64-472a-80ed-82e8f1e65575.json.crc

Notice that we have a directory structure laid out based on our ``daily,z2-2bit`` scheme. Notice the first parquet
file path is composed of a date path ``2016/05/06`` and then a z2 ordinate of ``2``, which is part of the file name ::

    /tmp/dstest/example-csv/2015/05/06/2/2_Wcec6a2ec594a4a2eb7c7980a1baf4ab3.parquet

The rest of the parquet file name is a UUID, which allows for multiple threads to write different files at once
without interference. If we ingested additional data, another file would be created under the partition, and
GeoMesa would scan them both at query time.

Each new file (or file deletion) will create a separate metadata file, which contains details on the file:

.. code-block:: bash

    $ cat /tmp/dstest/example-csv/metadata/update-2015-05-06-629788a4-6a70-4009-ae20-c45602a88483.json

The output should look like::

    {
        "action" : "Add",
        "count" : 1,
        "envelope" : {
            "xmax" : -100.2365,
            "xmin" : -100.2365,
            "ymax" : 23.0,
            "ymin" : 23.0
        },
        "files" : [
            "2_Wcec6a2ec594a4a2eb7c7980a1baf4ab3.parquet"
        ],
        "name" : "2015/05/06/2",
        "timestamp" : 1538148168948
    }
