.. _versions_and_downloads:

Versions and Downloads
======================

.. note::

    The current recommended version of GeoMesa is |release_last|.

GeoMesa requires `Java JRE or JDK 8`__ to run.

__ http://www.oracle.com/technetwork/java/javase/downloads/index.html

Release Distributions
---------------------

GeoMesa release distributions contain pre-built artifacts for using GeoMesa. They can be
downloaded from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Older versions can be downloaded from the `LocationTech Maven repository`__.

__ https://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa

Maven Integration
-----------------

GeoMesa is now hosted on Maven Central. However, it still depends on several third-party libraries only available
in other repositories. To include GeoMesa in your project, add the following repositories to your pom:

.. code-block:: xml

    <repositories>
      <repository>
        <id>boundlessgeo</id>
        <url>https://repo.boundlessgeo.com/main</url>
      </repository>
      <repository>
        <id>osgeo</id>
        <url>http://download.osgeo.org/webdav/geotools</url>
      </repository>
      <repository>
        <id>conjars.org</id>
        <url>http://conjars.org/repo</url>
      </repository>
    </repositories>

and then include the desired ``geomesa-*`` dependencies:


.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-utils_2.11</artifactId>
      <version>2.0.0-m.1</version>
    </dependency>

Snapshot artifacts are available in the LocationTech snapshots repository:

.. code-block:: xml

    <repository>
      <id>geomesa-snapshots</id>
      <url>https://repo.locationtech.org/content/repositories/geomesa-snapshots</url>
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

* `Java JDK 8 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__
* `Apache Maven <http://maven.apache.org/>`__ |maven_version|

Source can be cloned using `Git <http://git-scm.com/>`__ or downloaded from `GitHub`__.

__ https://github.com/locationtech/geomesa/archive/master.tar.gz

To build, change to the source directory and use Maven:

.. code-block:: bash

    mvn clean install

The full build takes quite a while. To speed it up, you may skip tests and use multiple threads. GeoMesa also
provides the script ``build/mvn``, which is a wrapper around Maven that downloads and runs
`Zinc <https://github.com/typesafehub/zinc>`__, a fast incremental compiler:

.. code-block:: bash

    build/mvn clean install -T8 -DskipTests

Upgrading
---------

For details on changes between versions, see :ref:`upgrade_guide`.

Legal Review
------------

GeoMesa is part of the Locationtech working group at Eclipse. The Eclipse legal team fully reviews
each major release for IP concerns. The latest release which has been fully reviewed by Eclipse Legal
is GeoMesa |eclipse_release|.

.. warning::

    Eclipse releases may not contain all the bug fixes and improvements from the latest release.

* Release distribution: |eclipse_release_tarball|
* Source: |eclipse_release_source_tarball|
