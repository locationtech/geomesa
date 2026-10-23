GeoMesa NiFi Quick Start
========================

This tutorial provides an example implementation for using GeoMesa with
NiFi. This walk-through will guide you in setting up the components
required for ingesting GDELT files into GeoMesa.

Prerequisites
-------------

This tutorial uses `Docker <https://docs.docker.com/get-docker/>`_, and assumes a Linux OS.

About this Tutorial
-------------------

This quick start operates by reading CSV files from the local filesystem, and writing them to a Redis
instance using the PutGeoMesa processor.

Download the GeoMesa NiFi NARs
------------------------------

First, we will download the appropriate NARs. Full instructions are available under :ref:`nifi_install`, but
the relevant sections are reproduced here. For this tutorial, we will be using three NARs:

* ``geomesa-datastore-services-nar``
* ``geomesa-datastore-services-api-nar``
* ``geomesa-redis-nar``

This tutorial will use the GeoMesa Redis data store to simplify setup, but any other back-end
store can be used instead by changing the ``DataStoreService`` used.

.. code-block:: bash

    $ mkdir -p ~/gm-nifi-quickstart/extensions
    $ cd ~/gm-nifi-quickstart
    $ export NARS="geomesa-redis-nar geomesa-datastore-services-api-nar geomesa-datastore-services-nar"
    $ for nar in $NARS; do wget -O "extensions/${nar}_{{scala_binary_version}}-{{release}}.nar" "https://github.com/geomesa/geomesa-nifi/releases/download/geomesa-nifi-{{release}}/${nar}_{{scala_binary_version}}-{{release}}.nar"; done

Obtain GDELT data
-----------------

The `GDELT Event database <https://www.gdeltproject.org>`__ provides a comprehensive time- and location-indexed
archive of events reported in broadcast, print, and web news media worldwide from 1979 to today. GeoMesa ships
with the ability to parse GDELT data, so it's a good data format for this tutorial. For more details,
see :ref:`gdelt_converter`.

Run the following commands to download a recent GDELT file:

.. code-block:: bash

    $ cd ~/gm-nifi-quickstart
    $ mkdir gdelt
    $ export GDELT_URL="$(wget -qO- 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt' | head -n 1 | awk '{ print $3 }')"
    $ wget "$GDELT_URL" -qO "gdelt/$(basename $GDELT_URL)"
    $ unzip -d gdelt gdelt/*.zip
    $ rm gdelt/*.zip

Then run the following to download the GeoMesa converter definition for GDELT. This defines how the GDELT data should be parsed:

.. code-block:: bash

    $ cd ~/gm-nifi-quickstart
    $ wget "https://raw.githubusercontent.com/locationtech/geomesa/refs/tags/geomesa-{{release}}/geomesa-tools/conf/sfts/gdelt/reference.conf" \
      -qO reference.conf

Run Redis with Docker
---------------------

We'll use Redis as our back-end data store, as it's lightweight and easy to set up.

First, we'll create a docker network so that Redis and NiFi can connect to each other:

.. code-block:: bash

    $ docker network ls | grep -q geomesa || { echo "Creating docker network"; docker network create geomesa; }

Next, use the following command to start up a Redis instance:

.. code-block:: bash

    $ docker run --rm -p 6379:6379 --network geomesa --hostname redis --name redis redis:latest

Run NiFi with Docker
--------------------

Next, we will run NiFi through Docker, mounting in our NARs. In a separate console, run:

.. code-block:: bash

    $ cd ~/gm-nifi-quickstart
    $ docker run --rm \
      --network geomesa \
      -p 8443:8443 \
      -e SINGLE_USER_CREDENTIALS_USERNAME=nifi \
      -e SINGLE_USER_CREDENTIALS_PASSWORD=nifipassword \
      -v "$PWD/extensions:/opt/nifi/nifi-current/nar_extensions:ro" \
      -v "$PWD/gdelt:/gdelt:ro" \
      -v "$PWD/reference.conf:/opt/nifi/nifi-current/conf/reference.conf:ro" \
      apache/nifi:2.4.0

Once NiFi has finished starting up, it will be available at ``https://localhost:8443/nifi``. You will likely have to
click through a certificate warning due to the default self-signed cert being used. Once in the NiFi UI, you can log
in with the credentials we specified in the run command; i.e. ``nifi``/``nifipassword``.

Create the NiFi Flow
--------------------

If you are not familiar with NiFi, follow the `Getting Started <https://nifi.apache.org/docs/nifi-docs/html/getting-started.html>`__
guide to familiarize yourself. The rest of this tutorial assumes a basic understanding of NiFi.

Add the ingest processor by dragging a new processor to your flow, and selecting ``PutGeoMesa``. Double-click the
processor to configure it. On the properties tab, select the three-dot menu next to ``DataStoreService`` and click
on "Create new service". There should be only one option, the ``RedisDataStoreService``, so
click the "Add" button. Next, select the three-dot menu next to the ``RedisDataStoreService`` entry and select "Go to service",
then select "Yes" when prompted to save changes. This should bring you to the Controller Services screen. Click
the three-dot menu next to the ``RedisDataStoreService`` and select "Edit". On the properties tab, enter the
following configuration:

* ``redis.url`` - ``redis:6379``
* ``redis.catalog`` - ``geomesa``

.. image:: /tutorials/_static/img/nifi-qs-redis-controller-config.png
   :align: center

Click "Apply", and the service should show as "validating". Click the "refresh" button in the bottom left of the
screen, and the service should show as "disabled". Select the three-dot menu next to the ``RedisDataStoreService``
entry, and click "Enable". Click the "Enable" button on the following dialog. Once enabled, close the dialog, then
click "Back to processor" in the top-left of the controller services. Click "Cancel" in the processor dialog, which
should bring you back to the main flow but keep our data store service configuration intact.

Now we will add two more processors to read our GDELT data. First, add a ``ListFile`` processor, and configure
the ``Input Directory`` to be ``/gdelt`` (the location of our mounted GDELT data). Next, add a ``FetchFile``
processor, and connect the output of ``ListFile`` to it.

Now we will create a process to set the attributes GeoMesa needs to ingest the data. Add an ``UpdateAttribute``
processor, and use the ``+`` button on the properties tab to add three dynamic properties. These properties define
the format of the input data:

* ``geomesa.converter`` - ``gdelt2``
* ``geomesa.sft.name`` - ``gdelt``
* ``geomesa.sft.spec`` - ``gdelt2``

.. image:: /tutorials/_static/img/nifi-qs-update-attributes.png
   :align: center

Next, connect the output of the ``FetchFile`` processor to the ``UpdateAttribute`` processor, and the output
of the ``UpdateAttribute`` processor to the ``PutGeoMesa`` processor. Auto-terminate any other relationships
that are still undefined (in a production system, we'd want to handle failures instead of ignoring them).

Now our flow is complete. It should look like the following:

.. image:: /tutorials/_static/img/nifi-qs-flow.png
   :align: center

Ingest the Data
---------------

We can start the flow by clicking on the background to de-select any processors, then clicking the "Play" button
on the left side of the NiFi UI. You should see the data pass through the NiFi flow and be ingested.

Visualize the Data
------------------

Once the data has been ingested, you can use GeoServer to visualize it on a map. Follow the instructions
in the File System data store quick-start tutorial, :ref:`redis_quickstart_visualize`.
