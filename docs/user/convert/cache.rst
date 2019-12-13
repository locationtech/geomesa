Using Caches for Enrichment
---------------------------

You can configure a literal cache or an external cache as a lookup table
to populate attributes based on an id in the data. This is useful for enriching
records with static information from an external source. In the example below,
we use the ``cacheLookup`` function to resolve the name of a person based on the
``number`` in each record. Each cache can be configured to expire records based
on an ``expiration`` parameter in milliseconds.

Config:

::

    {
      type          = "xml"
      id-field      = "uuid()"
      feature-path  = "Feature" // optional path to feature elements
      options = {
        line-mode = "multi" // or "single"
      }
      caches = {
        names = {
           type = "simple"
           data = {
             123 = {
                name = "Jane"
                email = "bar@baz.com"
             }
             148 = {
                name = "Mary"
                email = "foo@bar.com"
             }
           }
        }
      }
      fields = [
        { name = "number", path = "number",           transform = "$0::integer"       }
        { name = "color",  path = "color",            transform = "trim($0)"          }
        { name = "weight", path = "physical/@weight", transform = "$0::double"        }
        { name = "source", path = "/doc/DataSource/name/text()"                       }
        { name = "lat",    path = "geom/lat",         transform = "$0::double"        }
        { name = "lon",    path = "geom/lon",         transform = "$0::double"        }
        { name = "name",   transform = "cacheLookup('names', $number, 'name')"        }
        { name = "geom",                              transform = "point($lon, $lat)" }
      ]
    }

Data:

.. code-block:: xml

    <?xml version="1.0"?>
    <doc>
        <DataSource>
            <name>myxml</name>
        </DataSource>
        <Feature>
            <number>123</number>
            <geom>
                <lat>12.23</lat>
                <lon>44.3</lon>
            </geom>
            <color>red</color>
            <physical height="5'11" weight="127.5"/>
        </Feature>
        <Feature>
            <number>456</number>
            <geom>
                <lat>20.3</lat>
                <lon>33.2</lon>
            </geom>
            <color>blue</color>
            <physical height="h2" weight="150"/>
        </Feature>
    </doc>


To configure a Redis cache, specify the ``caches`` section as follows:

Config:

::

    {
      caches = {
        redis = {
           type = "redis"
           redis-url = "url_of_redis"
           expiration = 30000 // milliseconds
        }
      }
    }
