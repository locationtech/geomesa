.. _versions_and_downloads:

Versions and Downloads
======================

.. note::

    The current recommended version of GeoMesa is ``{{release}}``.

GeoMesa requires `Java`__ to run. GeoMesa currently supports Java {{java_supported_versions}}.

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

and then include the desired GeoMesa dependencies:

.. code-block:: xml

    <properties>
      <geomesa.version>{{release}}</geomesa.version>
      <scala.binary.version>{{scala_binary_version}}</scala.binary.version>
    </properties>

    <dependencies>
      <dependency>
        <groupId>org.locationtech.geomesa</groupId>
        <artifactId>geomesa-utils_${scala.binary.version}</artifactId>
        <version>${geomesa.version}</version>
      </dependency>
    </dependencies>

GeoMesa provides a bill-of-materials module, which can simplify version management:

.. code-block:: xml

    <dependencyManagement>
      <dependencies>
        <dependency>
          <groupId>org.locationtech.geomesa</groupId>
          <artifactId>geomesa-bom_${scala.binary.version}</artifactId>
          <version>${geomesa.version}</version>
          <type>pom</type>
          <scope>import</scope>
        </dependency>
      </dependencies>
    </dependencyManagement>

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

* `Java <https://adoptium.net/temurin/releases/>`__ JDK {{java_required_version}}
* `Apache Maven <https://maven.apache.org/>`__ {{maven_required_version}}

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
