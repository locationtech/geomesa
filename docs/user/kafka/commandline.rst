Kafka Command Line Tools
========================

Run ``geomesa-kafka`` without any arguments to produce the following usage text::

    $ geomesa-kafka
      INFO  Usage: geomesa-kafka [command] [command options]
        Commands:
          classpath          Display the GeoMesa classpath
          configure          Configure the local environment for GeoMesa
          create-schema      Create a GeoMesa feature type
          describe-schema    Describe the attributes of a given GeoMesa feature type
          get-type-names     List GeoMesa feature types for a given zk path
          help               Show help
          ingest             Ingest/convert various file formats into GeoMesa
          keywords           Add/Remove/List keywords on an existing schema
          listen             Listen to a GeoMesa Kafka topic
          remove-schema      Remove a schema and associated features from a GeoMesa zk path
          version            Display the GeoMesa version installed locally


This usage text lists the available commands. To see help for an individual command,
run ``geomesa-kafka help <command-name>``, which will give you more detailed information.

Command overview
----------------

classpath
~~~~~~~~~

Prints out the current classpath configuration::

    $ geomesa-kafka classpath

configure
~~~~~~~~~

Used to configure the current environment for using the commandline tools. This is frequently run after the tools are
first installed to ensure the environment is configured correctly::

    $ geomesa-kafka configure

create-schema
~~~~~~~~~~~~~

Used to create a simple feature type. This will also create a topic in Kafka::

    $ geomesa-kafka create-schema -f testfeature \
      -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092 \
      -s fid:String:index=true,dtg:Date,geom:Point:srid=4326

describe-schema
~~~~~~~~~~~~~~~

Describes a feature type::

    $ geomesa-kafka describe-schema -f testfeature \
      -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092

    INFO  Describing attributes of feature 'testfeature'
    fid  | String  (Attribute indexed)
    dtg  | Date    (Spatio-temporally indexed)
    geom | Point   (Spatially indexed)

    User data:
      geomesa.index.dtg   | dtg
      geomesa.kafka.topic | geomesa-ds-kafka-testfeature

get-type-names
~~~~~~~~~~~~~~

Displays any simple feature types that have been created::

    $ geomesa-kafka get-type-names \
      -z zoo1,zoo2,zoo3  -b broker1:9092,broker2:9092

    Current feature types:
    testfeature

help
~~~~

Show available commands, or show options for a specific command::

    $ geomesa-kafka help ingest

ingest
~~~~~~

Ingest files into GeoMesa. For a full description, see the Accumulo documentation :ref:`ingest`. In addition to
the options described there, the Kafka data store allows you to specify a delay between messages. This can be used
to simulate a live data stream::

    $ geomesa-kafka ingest \
      -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092 \
      -C example-csv -s example-csv \
      --delay 5s  examples/ingest/csv/example.csv

keywords
~~~~~~~~

List, add or remove keywords in a specified schema::

    $ geomesa-kafka keywords --list -f testfeature \
      -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092

listen
~~~~~~

Read messages from Kafka. The ``listen`` command will maintain a connection to Kafka, and display
any messages written by a Kafka data store (for example, with the ``ingest`` command). Messages
that were previously written to Kafka can be displayed with the ``--from-beginning`` flag::

    $ geomesa-kafka listen -f testfeature \
      -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092 \
      --from-beginning

remove-schema
~~~~~~~~~~~~~

Deletes a simple feature type, and the associated topic (if Kafka topic deletion is enabled)::

    $ geomesa-kafka remove-schema -f testfeature \
      -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092

version
~~~~~~~

Lists the version, git branch and commit ID of GeoMesa being run::

    $ geomesa-kafka version
