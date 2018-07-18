.. _installing_sft_and_converter_definitions:

Using SFT and Converter Definitions with Command-Line Tools
-----------------------------------------------------------

The GeoMesa binary distributions ship with prepackaged feature type and
converter definitions for common data types including Twitter, GeoNames, T-drive, and
several more. These converters can be used with the GeoMesa command-line tools out of the box.
See :ref:`prepackaged_converters`. In addition, common file formats such as GeoJSON, delimited text,
or self-describing Avro can often be ingested without a converter. See :ref:`cli_ingest` for details.

Users can add additional SimpleFeatureType and converter types by providing a ``reference.conf`` file
embedded with a JAR within the ``lib`` directory, or by adding the types to the
``application.conf`` file in the ``conf`` directory of the tools distribution.

.. note::

    The example below is specific to the GeoMesa Accumulo distribution, but the
    general principle is the same for each distribution. Only the home variable and
    command-line tool name will differ depending on GeoMesa distribution.

Given the following sample CSV file ``example.csv``:

::

    ID,Name,Age,LastSeen,Friends,Lat,Lon
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan",-100.236523,23
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry",40.232,-53.2356
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort",3,-62.23

A "renegades" SFT and "renegades-csv" converter may be specified in
the GeoMesa Tools configuration file (``$GEOMESA_ACCUMULO_HOME/conf/application.conf``)
as shown below. By default, SFTs will be loaded from the file
at the path ``geomesa.sfts`` and converters will be loaded at the path
``geomesa.converters``. Each converter and SFT definition is keyed by the name that
can be referenced in the converter and SFT loaders.

``$GEOMESA_ACCUMULO_HOME/conf/application.conf``:

::

    geomesa = {
      sfts = {
         # other SFTs
         # ...
        "renegades" = {
          attributes = [
            { name = "fid",      type = "Integer",      index = false                             }
            { name = "name",     type = "String",       index = true                              }
            { name = "age",      type = "Integer",      index = false                             }
            { name = "lastseen", type = "Date",         index = true                              }
            { name = "friends",  type = "List[String]", index = true                              }
            { name = "geom",     type = "Point",        index = true, srid = 4326, default = true }
          ]
        }
      }
      converters = {
         # other converters
         # ...
        "renegades-csv" = {
          type = "delimited-text",
          format = "CSV",
          options {
            skip-lines = 1
          },
          id-field = "toString($fid)",
          fields = [
            { name = "fid",      transform = "$1::int"                 }
            { name = "name",     transform = "$2::string"              }
            { name = "age",      transform = "$3::int"                 }
            { name = "lastseen", transform = "date('yyyy-MM-dd', $4)"  }
            { name = "friends",  transform = "parseList('string', $5)" }
            { name = "lon",      transform = "$6::double"              }
            { name = "lat",      transform = "$7::double"              }
            { name = "geom",     transform = "point($lon, $lat)"       }
          ]
        }
      }
    }


Use ``geomesa-accumulo env`` to confirm that ``geomesa-accumulo ingest`` can properly read
the updated file.

.. code-block:: shell

    $ geomesa-accumulo env

Once the converter and SFT are registered, it can be used to ingest the
``example.csv`` file:

.. code-block:: shell

    $ geomesa-accumulo ingest -u <user> -p <pass> -i <instance> -z <zookeepers> -s renegades -C renegades-csv  example.csv
