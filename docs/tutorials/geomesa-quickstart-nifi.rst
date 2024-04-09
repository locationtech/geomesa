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

This quick start operates by reading CSV files from the local filesystem, and writing them to GeoMesa
Parquet files using the PutGeoMesa processor.

Download the GeoMesa NiFi NARs
------------------------------

First, we will download the appropriate NARs. Full instructions are available under :ref:`nifi_install`, but
the relevant sections are reproduced here. For this tutorial, we will be using three NARs:

* ``geomesa-datastore-services-nar``
* ``geomesa-datastore-services-api-nar``
* ``geomesa-fs-nar``

This tutorial will use the GeoMesa FileSystem data store to avoid external dependencies, but any other back-end
store can be used instead by changing the ``DataStoreService`` used.

First, set the version to use:

.. parsed-literal::

    export TAG="|release_version|"
    export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version

.. code-block:: bash

    mkdir -p ~/gm-nifi-quickstart/extensions
    cd ~/gm-nifi-quickstart
    export NARS="geomesa-fs-nar geomesa-datastore-services-api-nar geomesa-datastore-services-nar"
    for nar in $NARS; do wget -O "extensions/${nar}_$VERSION.nar" "https://github.com/geomesa/geomesa-nifi/releases/download/geomesa-nifi-$TAG/${nar}_$VERSION.nar"; done

Obtain GDELT data
-----------------

<<<<<<< HEAD
The `GDELT Event database <https://www.gdeltproject.org>`__ provides a comprehensive time- and location-indexed
archive of events reported in broadcast, print, and web news media worldwide from 1979 to today. GeoMesa ships
with the ability to parse GDELT data, so it's a good data format for this tutorial. For more details,
=======
In this tutorial we will be ingesting GDELT data. If you already have some GDELT data downloaded, then
you may skip this section.

The `GDELT Event database <http://www.gdeltproject.org>`__ provides a comprehensive time- and location-indexed
archive of events reported in broadcast, print, and web news media worldwide from 1979 to today. The raw GDELT
version 1 data files are available to download at http://data.gdeltproject.org/events/index.html. The version 2
files can be downloaded at http://data.gdeltproject.org/gdeltv2/masterfilelist.txt.

GeoMesa ships with the ability to parse GDELT data, and a script for downloading it. For more details,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
see :ref:`gdelt_converter`.

Run the following commands to download a recent GDELT file:

.. code-block:: bash

    cd ~/gm-nifi-quickstart
    mkdir gdelt
    export GDELT_URL="$(wget -qO- 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt' | head -n 1 | awk '{ print $3 }')"
    wget "$GDELT_URL" -qO- "gdelt/$(basename $GDELT_URL)"
    unzip -d gdelt gdelt/*.zip
    rm gdelt/*.zip

Run NiFi with Docker
--------------------

Next, we will run NiFi through Docker, mounting in our NARs and a directory for writing out data:

.. code-block:: bash

    cd ~/gm-nifi-quickstart
    mkdir fs
    docker run --rm \
      -p 8443:8443 \
      -e SINGLE_USER_CREDENTIALS_USERNAME=nifi \
      -e SINGLE_USER_CREDENTIALS_PASSWORD=nifipassword \
      -v "$(pwd)/extensions:/opt/nifi/nifi-current/extensions:ro" \
      -v "$(pwd)/fs:/fs:rw" \
      -v "$(pwd)/gdelt:/gdelt:ro" \
      apache/nifi:1.19.1

Once NiFi has finished starting up, it will be available at ``https://localhost:8443/nifi``. You will likely have to
click through a certificate warning due to the default self-signed cert being used. Once in the NiFi UI, you can log
in with the credentials we specified in the run command; i.e. ``nifi``/``nifipassword``.

Create the NiFi Flow
--------------------

If you are not familiar with NiFi, follow the `Getting Started <https://nifi.apache.org/docs/nifi-docs/html/getting-started.html>`__
guide to familiarize yourself. The rest of this tutorial assumes a basic understanding of NiFi.

Add the ingest processor by dragging a new processor to your flow, and selecting ``PutGeoMesa``. Select the
processor and click the 'configure' button to configure it. On the properties tab, select ``DataStoreService``
and click on "Create new service". There should be only one option, the ``FileSystemDataStoreService``, so
click the "Create" button. Next, click the small arrow next to the ``FileSystemDataStoreService`` entry, and
select "Yes" when prompted to save changes. This should bring you to the Controller Services screen. Click
the small gear next to the ``FileSystemDataStoreService`` to configure it. On the properties tab, enter the
following configuration:

* ``fs.path`` - ``/fs``
* ``fs.encoding`` - ``parquet``

.. image:: /tutorials/_static/img/nifi-qs-fs-controller-config.png
   :align: center

Click "Apply", and the service should show as "validating". Click the "refresh" button in the bottom left of the
screen, and the service should show as "disabled". Click the small lightning bolt next to the configure gear, and
the click the "Enable" button to enable it. Once enabled, close the dialog, then close the controller services
page by clicking the ``X`` in the top right. This should bring you back to the main flow.

Now we will add two more processors to read our GDELT data. First, add a ``ListFile`` processor, and configure
the ``Input Directory`` to be ``/gdelt`` (the location of our mounted GDELT data). Next, add a ``FetchFile``
processor, and connect the output of ``ListFile`` to it.

Now we will create a process to set the attributes GeoMesa needs to ingest the data. Add an ``UpdateAttribute``
processor, and use the ``+`` button on the properties tab to add four dynamic properties:

* ``geomesa.converter`` - ``gdelt2``
* ``geomesa.sft.name`` - ``gdelt``
* ``geomesa.sft.spec`` - ``gdelt2``
* ``geomesa.sft.user-data`` - ``geomesa.fs.scheme={"name":"daily","options":{"dtg-attribute":"dtg"}}``

.. image:: /tutorials/_static/img/nifi-qs-update-attributes.png
   :align: center

The first three properties define the format of the input data. The last property is used by the GeoMesa File System
data store to partition the data on disk. See :ref:`fsds_partition_schemes` for more information.

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
in the File System data store quick-start tutorial, :ref:`fsds_quickstart_visualize`.

Note that due to Docker file permissions, you may need to run something like the following to make the data
accessible:

.. code-block:: bash

    cd ~/gm-nifi-quickstart
    docker run --rm \
      -v "$(pwd)/fs:/fs:rw" \
      --entrypoint bash \
      apache/nifi:1.19.1 \
      -c "chmod -R 777 /fs"
