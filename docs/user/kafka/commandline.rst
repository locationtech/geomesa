Kafka Command Line Tools
========================

Run ``geomesa-kafka`` without any arguments to produce the following usage text::

    $ geomesa-kafka
      Usage: geomesa-kafka [command] [command options]
        Commands:
          classpath       Display the GeoMesa classpath
          configure       Configure the local environment for GeoMesa
          convert         Convert files using GeoMesa's internal SFT converter framework
          create-schema   Create a feature definition in GeoMesa
          get-schema      Describe the attributes of a given feature in GeoMesa
          get-names       List GeoMesa features for a given zkPath
          help            Show help
          listen          Listen to a GeoMesa Kafka topic
          remove-schema   Remove a schema and associated features from GeoMesa
          version         GeoMesa Version

This usage text lists the available commands. To see help for an individual command,
run ``geomesa-kafka help <command-name>``, which for example will give you something like this::

    $ geomesa-kafka help get-names
      List GeoMesa features for a given zkPath
      Usage: get-names [options]
        Options:
        * -b, --brokers
             Brokers (host:port, comma separated)
          -p, --zkpath
             Zookeeper path where feature schemas are saved
        * -z, --zookeepers
             Zookeepers (host[:port], comma separated)

Command overview
----------------

configure
~~~~~~~~~

Used to configure the current environment for using the commandline tools. This is frequently run after the tools are
first installed to ensure the environment is configured correctly::

    $ geomesa configure

classpath
~~~~~~~~~

Prints out the current classpath configuration::

    $ geomesa classpath

create-schema
~~~~~~~~~~~~~

Used to create a feature type (``SimpleFeatureType``) at the specified zkpath::

    $ geomesa-kafka create-schema -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 \
      -p /geomesa/ds/kafka

get-schema
~~~~~~~~~~

Display details about the attributes of a specified feature type::

    $ geomesa-kafka get-schema -f testfeature -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 -p /geomesa/ds/kafka

get-names
~~~~~~~~~

List all known feature types in Kafka::

    $ geomesa-kafka get-names -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092

If no ``--zkpath`` parameter is specified, the ``get-names`` command will search all of zookeeper for potential feature types.

listen
~~~~~~

Logs out the messages written to a topic corresponding to the feature type passed in.

    $ geomesa-kafka listen -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka \
      --from-beginning

remove-schema
~~~~~~~~~~~~~

Used to remove a feature type (``SimpleFeatureType``) in a GeoMesa catalog. This will also delete any feature of that type in the data store::

    $ geomesa-kafka remove-schema -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka
    $ geomesa-kafka remove-schema --pattern 'testfeature\d+' \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka

version
~~~~~~~

Prints out the version, git branch, and commit ID that the tools were built with::

    $ geomesa version

Formatting GeoMesa Kafka Messages
---------------------------------

The ``KafkaGeoMessageFormatter`` class,
part of the ``geomesa-kafka-datastore`` module, may be
used with the ``kafka-console-consumer`` Kafka command-line tool. In order
to use this formatter, call ``kafka-console-consumer`` with these
additional arguments:

.. note::

    Replace ``$KAFKAVERSION`` below with the appropriate version number for your environment: 08, 09, or 10.
    e.g. ``org.locationtech.geomesa.kafka08.KafkaGeoMessageFormatter``

.. code-block:: bash

    --formatter org.locationtech.geomesa.kafka$KAFKAVERSION.KafkaGeoMessageFormatter
    --property sft.name={sftName}
    --property sft.spec={sftSpec}

In order to pass the spec via a command argument all ``%`` characters
must be replaced by ``%37`` and all ``=`` characters must be replaced by
``%61``.

A slightly easier to use but slightly less flexible alternative is to
use the ``KafkaDataStoreLogViewer`` instead of the
``kafka-console-consumer``. To use the ``KafkaDataStoreLogViewer`` first
copy the geomesa-kafka-geoserver-plugin.jar to $KAFKA\_HOME/libs. Then
create a copy of $KAFKA\_HOME/bin/kafka-console-consumer.sh called
"kafka-ds-log-viewer" and in the copy replace the classname in the exec
command at the end of the script with
``org.locationtech.geomesa.kafka$KAFKAVERSION.KafkaDataStoreLogViewer``.

The ``KafkaDataStoreLogViewer`` requires three arguments:
``--zookeeper``, ``--zkPath``, and ``--sftName``. It also supports an
optional argument ``--from`` which accepts values ``oldest`` and
``newest``. ``oldest`` is equivalent to specifying ``--from-beginning``
when using the ``kafka-console-consumer`` and ``newest`` is equivalent
to not specifying ``--from-beginning``.

For example:

.. code-block:: bash

    $ kafka-ds-log-viewer --zookeeper {zookeeper} --zkPath {zkPath} --sftName {sftName}

The ``KafkaDataStoreLogViewer`` loads the ``SimpleFeatureType`` from
Zookeeper so it does not need to be passed via the command line.