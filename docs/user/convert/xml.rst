Parsing XML
-----------

The XML converter defines each field using XPath expressions. For XML documents with multiple features,
the ``feature-path`` element can be used to select feature elements. In this case, the attribute paths will
be relevant to the feature element. The optional ``xsd`` element can be used to validate input files against
an XML schema.

By default, the XML converter will treat each line of input as a single XML document. The ``line-mode`` option
can be used to parse the entire input as a single document instead of line-by-line. Note that multi-line parsing
will read the entire input into memory, so should not be used with large files.

The XML converter will attempt to use the Saxon XPath factory if it is available. In GeoMesa tools, a script is
provided to download saxon - ``bin/install-saxon.sh``. To specify an alternate XPath factory, use the ``xpath-factory``
option. If the factory can not be loaded, the default Java factory will be used - note that this can be
significantly slower.

Example XML:

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

Config:

::

    {
      type          = "xml"
      id-field      = "uuid()"
      feature-path  = "Feature" // optional path to feature elements
      xsd           = "example.xsd" // optional xsd file to validate input
      xpath-factory = "net.sf.saxon.xpath.XPathFactoryImpl"
      options = {
        line-mode = "multi" // or "single"
      }
      fields = [
        { name = "number", path = "number",           transform = "$0::integer"       }
        { name = "color",  path = "color",            transform = "trim($0)"          }
        { name = "weight", path = "physical/@weight", transform = "$0::double"        }
        { name = "source", path = "/doc/DataSource/name/text()"                       }
        { name = "lat",    path = "geom/lat",         transform = "$0::double"        }
        { name = "lon",    path = "geom/lon",         transform = "$0::double"        }
        { name = "geom",                              transform = "point($lon, $lat)" }
      ]
    }


Handling Namespaces with Saxon
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using the default XPath factory, XML namespaces can generally be ignored. However, the Saxon factory
requires namespaces to be declared. You can accomplish this through the ``xml-namespaces`` configuration.

Example XML:

.. code-block:: xml

    <?xml version="1.0"?>
    <foo:doc xmlns:foo="http://example.com/foo" xmlns:bar="http://example.com/bar">
        <foo:DataSource>
            <foo:name>myxml</foo:name>
        </foo:DataSource>
        <foo:Feature>
            <foo:number>123</foo:number>
            <bar:geom>
                <bar:lat>12.23</bar:lat>
                <bar:lon>44.3</bar:lon>
            </bar:geom>
            <foo:color>red</foo:color>
            <foo:physical height="5'11" weight="127.5"/>
        </foo:Feature>
    </foo:doc>

Config:

::

    {
      type          = "xml"
      id-field      = "uuid()"
      feature-path  = "foo:Feature" // optional path to feature elements
      xsd           = "example.xsd" // optional xsd file to validate input
      xpath-factory = "net.sf.saxon.xpath.XPathFactoryImpl"
      options = {
        line-mode = "multi" // or "single"
      }
      xml-namespaces = {
        foo = "http://example.com/foo"
        bar = "http://example.com/bar"
      }
      fields = [
        { name = "number", path = "foo:number",           transform = "$0::integer"       }
        { name = "color",  path = "foo:color",            transform = "trim($0)"          }
        { name = "weight", path = "foo:physical/@weight", transform = "$0::double"        }
        { name = "source", path = "/foo:doc/foo:DataSource/foo:name/text()"               }
        { name = "lat",    path = "bar:geom/bar:lat",     transform = "$0::double"        }
        { name = "lon",    path = "bar:geom/bar:lon",     transform = "$0::double"        }
        { name = "geom",                                  transform = "point($lon, $lat)" }
      ]
    }

