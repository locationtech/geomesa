Extending the Converter Library
-------------------------------

There are two ways to extend the converter library - adding new
transformation functions and adding new data formats.

Adding New Transformation Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To add new transformation functions, create a
``TransformationFunctionFactory`` and register it in
``META-INF/services/org.locationtech.geomesa.convert.TransformationFunctionFactory``.
For example, here's how to add a new transformation function that
computes a SHA-256 hash.

.. code-block:: scala

    import org.locationtech.geomesa.convert.TransformerFunctionFactory
    import org.locationtech.geomesa.convert.TransformerFn

    class SHAFunctionFactory extends TransformerFunctionFactory {
      override def functions = Seq(sha256fn)
      val sha256fn = TransformerFn("sha256") { args =>
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
``org.locationtech.geomesa.convert.avro.Avro2SimpleFeatureConverter``
for an example.