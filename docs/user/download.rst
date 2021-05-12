.. _versions_and_downloads:

Versions and Downloads
======================

.. note::

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    The current recommended version of GeoMesa is |release_version_literal|.
=======
    The current recommended version of GeoMesa is |release_version|.
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    The current recommended version of GeoMesa is |release_version_literal|.
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
    The current recommended version of GeoMesa is |release_version_literal|.
=======
    The current recommended version of GeoMesa is |release_version|.
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

GeoMesa requires `Java`__ to run. GeoMesa supports Java LTS versions |java_supported_versions|.

__ https://adoptium.net/temurin/releases/

Release Distributions
---------------------

GeoMesa release distributions contain binary artifacts for using GeoMesa. They can be
downloaded from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

<<<<<<< HEAD
=======
Older versions can be downloaded from the `Eclipse Maven repository`__.

__ https://repo.eclipse.org/content/repositories/geomesa-releases/org/locationtech/geomesa

>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
Maven Integration
-----------------

GeoMesa artifacts are hosted on Maven Central. However, there are several required third-party libraries
that are only available in other repositories. To include GeoMesa in your project, add the following
repositories to your pom:

.. code-block:: xml

    <repositories>
      <!-- geotools -->
      <repository>
        <id>osgeo</id>
        <url>https://repo.osgeo.org/repository/release</url>
      </repository>
      <!-- confluent -->
      <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
      </repository>
    </repositories>

and then include the desired ``geomesa-*`` dependencies:

.. parsed-literal::

    <properties>
      <geomesa.version>\ |release_version|\ </geomesa.version>
<<<<<<< HEAD
      <scala.binary.version>\ |scala_binary_version|\ </scala.binary.version>
=======
      <scala.abi.version>\ |scala_binary_version|\ </scala.abi.version>
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
    </properties>

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      <artifactId>geomesa-utils_${scala.binary.version}</artifactId>
      <version>${geomesa.version}</version>
=======
      <artifactId>geomesa-utils_2.12</artifactId>
      <version>3.0.0</version>
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      <artifactId>geomesa-utils_${scala.abi.version}</artifactId>
      <version>${geomesa.version}</version>
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
      <artifactId>geomesa-utils_${scala.abi.version}</artifactId>
      <version>${geomesa.version}</version>
=======
      <artifactId>geomesa-utils_2.12</artifactId>
      <version>3.0.0</version>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    </dependency>

For cutting-edge development, nightly snapshots are available from Eclipse:

.. code-block:: xml

    <repository>
      <id>geomesa-snapshots</id>
      <url>https://repo.eclipse.org/content/repositories/geomesa-snapshots</url>
      <releases>
        <enabled>false</enabled>
      </releases>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
    </repository>

Source Code
-----------

To build and install the source distribution requires:

* `Java JDK 11 <https://adoptium.net/temurin/releases/>`__
* `Apache Maven <https://maven.apache.org/>`__ |maven_version|

Source can be cloned using `Git <https://git-scm.com/>`__ or downloaded from `GitHub`__.

__ https://github.com/locationtech/geomesa/archive/main.tar.gz

To build, change to the source directory and use Maven:

.. code-block:: bash

    $ mvn clean install

The full build takes quite a while. To speed it up, you may skip tests and use multiple threads. GeoMesa also
provides the script ``build/mvn``, which is a wrapper around Maven that downloads and runs
`Zinc <https://github.com/typesafehub/zinc>`__, a fast incremental compiler:

.. code-block:: bash

    $ build/mvn clean install -T8 -DskipTests

Upgrading
---------

For details on changes between versions, see the :ref:`upgrade_guide`.
