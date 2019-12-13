Extending the Converter Library
-------------------------------

There are two ways to extend the converter library - adding new
transformation functions and adding new data formats.

Adding Scripting Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~

The easiest way to extend functionality is by defining custom Javascript functions. See
:ref:`convert_scripting_functions` for more details.

Adding New Transformation Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To add new transformation functions, create a
``TransformationFunctionFactory`` and register it in
``META-INF/services/org.locationtech.geomesa.convert2.transforms.TransformerFunctionFactory``.
For example, here's how to add a new transformation function that
computes a SHA-256 hash.

.. code-block:: scala

    import org.locationtech.geomesa.convert2.transforms.TransformerFunctionFactory
    import org.locationtech.geomesa.convert2.transforms.TransformerFunction

    class SHAFunctionFactory extends TransformerFunctionFactory {
      override def functions = Seq(sha256fn)
      val sha256fn = TransformerFunction("sha256") { args =>
        Hashing.sha256().hashBytes(args(0).asInstanceOf[Array[Byte]])
      }
    }

The ``sha256`` function can then be used in a field as shown.

::

       fields: [
          { name = "hash", transform = "sha256(stringToBytes($0))" }
       ]

Adding New Data Formats
~~~~~~~~~~~~~~~~~~~~~~~

To add new data formats, implement the ``SimpleFeatureConverterFactory``
and ``SimpleFeatureConverter`` interfaces and register them in
``META-INF/services`` appropriately. See
``org.locationtech.geomesa.convert.text.DelimitedTextConverter``
for an example.

Adding Functions to the Geomesa Classpath
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After creating a JAR file with your transformation function and factory
you can add these to the ``GEOMESA_EXTRA_CLASSPATHS`` environmental variable
in order to expose them to the command line tools and distributed (map-reduce)
ingest jobs.

A example of ingest with a transforms on the classpath is below:

.. code-block:: bash

    GEOMESA_EXTRA_CLASSPATHS="/tmp/custom-transformer-1.0.0.jar" bin/geomesa-accumulo ingest -u <user-name>
    -p <password> -s <sft-name> -C <converter-name> -c geomesa.catalog hdfs://localhost:9000/data/example.csv

You can also verify the classpath is properly configured with the tools:

.. code-block:: bash

    GEOMESA_EXTRA_CLASSPATHS="/tmp/custom-transformer-1.0.0.jar" bin/geomesa-accumulo classpath