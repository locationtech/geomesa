.. _create_kafka_ds_geoserver:

Using the Kafka Data Store in GeoServer
=======================================

.. note::

    For general information on working with GeoMesa GeoServer plugins,
    see :doc:`/user/geoserver`.

From the main GeoServer page, create a new store by either clicking
"Add stores" in the middle of the **Welcome** page, or anywhere in the
interface by clicking "Data > Stores" in the left-hand menu and then
clicking "Add new Store".

If you have properly installed the GeoMesa Kafka GeoServer plugin as described
in :ref:`install_kafka_geoserver`, "Kafka (GeoMesa)" should be included in the list
under **Vector Data Sources**. If you do not see this, check that you unpacked the
plugin JARs into in the right directory and restart GeoServer.

On the "Add Store" page, select "Kafka (GeoMesa)", and fill out the
parameters. The parameters are described in :ref:`kafka_parameters`.

Click "Save", and GeoServer will search Zookeeper for any GeoMesa-managed feature types.

.. _kafka_readiness_gs:

Kafka Layer Readiness Endpoint
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When a Kafka data store is configured with a read-back (``kafka.consumer.read-back``), it may take some time before layers
are available for querying. GeoMesa provides an endpoint that can be used to track the status of the consumer read-back.
When installed, GeoServer will expose an endpoint at ``rest/kafka`` (e.g. ``localhost:8080/geoserver/rest/kafka``) that can
be used in a readiness probe, returning HTTP status code ``200`` when all layers are loaded, and ``503`` when layers are not
yet available. See :ref:`install_kafka_readiness_gs` for installation instructions.

.. warning::

    ``kafka.consumer.start-on-demand`` must be set to false (unchecked) in order for layers to be automatically loaded when
    GeoServer starts up.
