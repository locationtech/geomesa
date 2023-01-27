.. _versions_and_downloads:

Versions and Downloads
======================

.. note::

    The current recommended version of GeoMesa is |release_version_literal|.

GeoMesa requires `Java`__ to run. GeoMesa supports Java LTS versions 8, 11 and 17.

__ https://adoptium.net/temurin/releases/

Release Distributions
---------------------

GeoMesa release distributions contain binary artifacts for using GeoMesa. They can be
downloaded from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

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
      <scala.binary.version>\ |scala_binary_version|\ </scala.binary.version>
    </properties>

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-utils_${scala.binary.version}</artifactId>
      <version>${geomesa.version}</version>
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

* `Java JDK 8 <https://adoptium.net/temurin/releases/>`__
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
