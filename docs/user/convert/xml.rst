.. _xml_converter:

XML Converter
=============

The XML converter handles XML files. To use the XML converter, specify ``type = "xml"`` in your converter
definition.

Configuration
-------------

The XML converter supports parsing files in single-line mode or in multi-line mode. In single-line mode, each line
of an input file should be a valid XML document; in multi-line mode, the entire input file should be a single valid
XML document. In order to support XPath expressions, each XML document is fully parsed into memory.
For large documents, this may take considerable time and memory. Thus, it is usually better to use single-line mode
when possible. Line mode may be specified by ``options.line-mode = "single"`` or ``options.line-mode = "multi"``
in your converter definition. If nothing is specified, single-line mode is used.

The XML converter will attempt to use the Saxon XML factory by default, if available. Saxon is generally much
faster than the default Java implementation. If desired, the parser may be specified by setting ``xpath-factory``
to the fully-qualified class name of a ``javax.xml.xpath.XPathFactory`` implementation.

If the XML source documents contain namespaces, you may need to define them through the ``xml-namespaces`` element.
See :ref:`xml_converter_namespaces`, below.

The XML converter supports schema validation through XSD documents. To enable validation, specify an XSD file
using the ``xsd`` element. This file must be available on the classpath. Input documents which don't validate
against the XSD will raise an exception.

Since a single XML document may contain multiple features, the XML parser supports an XPath expression pointing
to each feature element. This can be specified using the ``feature-path`` element.

The ``fields`` element in an XML converter supports an additional attribute, ``path``. ``path`` should be a XPath
expression, which may be relative to the ``feature-path`` (if defined, above) or absolute to the document root.
The ``path`` expression will be evaluated to a string, and be available in the ``transform`` element as ``$0``.

Transform Functions
-------------------

The ``transform`` element supports referencing the result of the ``path`` expression through ``$0``. Each value will
be a string.

In addition to the standard functions in :ref:`converter_functions`, the XML converter provides the following
XML-specific functions:

xmlToString
~~~~~~~~~~~

This will convert an XML element to a string. It can be useful for quickly representing a complex object, for
example in order to create a feature ID based on the hash of a row.

Example Usage
-------------

Assume the following SimpleFeatureType:

::

  geomesa.sfts.example = {
    attributes = [
      { name = "number", type = "Integer" }
      { name = "color",  type = "String"  }
      { name = "weight", type = "Double"  }
      { name = "source", type = "String"  }
      { name = "geom",   type = "Point"   }
    ]
  }

And the following XML document:

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

You could ingest with the following converter:

::

  geomesa.converters.myxml = {
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


.. _xml_converter_namespaces:

Handling Namespaces with Saxon
------------------------------

Using the default Java XPath factory, XML namespaces can generally be ignored. However, the Saxon factory
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

