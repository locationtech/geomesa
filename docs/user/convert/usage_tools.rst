Command-Line Tools
------------------

When using the GeoMesa command-line tools, many common file types such as GeoJSON, CSV, TSV, Avro, Parquet, and XML can be
ingested without defining a converter. Alternatively, converter and simple feature type definitions can be added to the classpath
using ``conf/reference.conf`` and/or ``conf/application.conf``, or by bundling an ``application.conf`` file into a JAR and
placing it in the ``lib`` directory. The provided ``conf/application.conf`` file has some sample converters that can be used for
testing, but it can be safely deleted and/or replaced.

For testing, the :ref:`cli_convert` command can be useful to verify converter output without persisting it to a data store.
The :ref:`cli_env` command can be used to list converters and feature type definitions from the classpath. The :ref:`cli_ingest`
command page contains additional details on using converters.
