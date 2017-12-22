GeoDocker: Local GeoMesa Accumulo
=================================

Getting started with spatio-temporal analysis with GeoMesa and Accumulo is incredibly simple, thanks to the
`GeoDocker-GeoMesa`_ project. The following guide describes how to bootstrap a GeoMesa Accumulo cluster on your local
machine using `Docker`_ in order to ingest and query some sample data.

.. _GeoDocker-GeoMesa: https://github.com/geodocker/geodocker-geomesa
.. _Docker: https://www.docker.com/

Prerequisites
-------------

Before you begin, you must have the following installed and configured:

-  a GitHub client,
-  `Docker Engine`_
-  `Docker Compose`_

.. _Docker Engine: https://docs.docker.com/engine/installation/
.. _Docker Compose: https://docs.docker.com/compose/install/

Bootstrap the cluster
---------------------

Pre-built images are available on `quay.io <https://quay.io/organization/geomesa>`_.

To create a GeoMesa Accumulo instance, clone the GeoDocker-GeoMesa project:

.. code:: bash

    $ git clone git@github.com:geodocker/geodocker-geomesa.git

Execute the following commands:

.. code:: bash

    $ cd geodocker-geomesa/geodocker-accumulo-geomesa
    $ docker-compose up

This will create a small cluster consisting of HDFS, Zookeeper, Accumulo and GeoServer. The GeoMesa
distributed-runtime jar is installed in the Accumulo classpath. The GeoMesa command line tools are
installed in the Accumulo master instance under `/opt/geomesa/`.

Accumulo is available on the standard ports, and GeoServer is available on port `9090`.

Quick Start
-----------

To connect to the Accumulo master instance, run:

.. code:: bash

    $ docker exec -ti geodockeraccumulogeomesa_accumulo-master_1 /usr/bin/bash

To ingest some sample data, use the GeoMesa command line tools:

.. code:: bash

    $ cd /opt/geomesa/bin
    $ ./geomesa-accumulo ingest -C example-csv -c example -u root -p GisPwd -s example-csv ../examples/ingest/csv/example.csv

To export data:

.. code:: bash

    $ ./geomesa-accumulo export -c example -u root -p GisPwd -f example-csv

To view data in GeoServer, go to ``http://localhost:9090/geoserver/web``, login with ``admin:geoserver``, click
'Stores' in the left gutter, then 'Add new store', then ``Accumulo (GeoMesa)``. Use the following parameters:

* accumulo.instance.id = accumulo
* accumulo.zookeepers = zookeeper
* accumulo.user = root
* accumulo.password = GisPwd
* accumulo.catalog = example (from the ingest command above)

Click 'save'. You should see the 'example-csv' layer available to publish.
