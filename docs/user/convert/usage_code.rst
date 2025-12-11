Programmatic Use
----------------

Converters and SimpleFeatureTypes can be imported through Maven and used directly in code:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <!-- pull in all converters, or use a specific converter, e.g. geomesa-convert-json_2.12 -->
      <artifactId>geomesa-convert-all_2.12</artifactId>
    </dependency>

.. tabs::

    .. code-tab:: scala

        import org.locationtech.geomesa.convert2.SimpleFeatureConverter

        val converter = SimpleFeatureConverter("example-csv", "example-csv") // load by-name from the classpath
        try {
          val is: InputStream = ??? // load your input data
          val features = converter.process(is)
          try {
            features.foreach(???) // do something with the conversion result
          } finally {
            features.close() // will also close the input stream
          }
        } finally {
          converter.close() // clean up any resources associated with your converter
        }

    .. code-tab:: java

        import org.geotools.api.feature.simple.SimpleFeature;
        import org.locationtech.geomesa.convert.EvaluationContext;
        import org.locationtech.geomesa.convert2.SimpleFeatureConverter;
        import org.locationtech.geomesa.utils.collection.CloseableIterator;
        import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader;

        import java.io.InputStream;
        import java.util.Collections;

        // load by-name from the classpath
        // use try-with-resources to clean up the converter when we're done
        try (SimpleFeatureConverter converter = SimpleFeatureConverter.apply("example-csv", "example-csv")) {
            EvaluationContext context = converter.createEvaluationContext(Collections.emptyMap());
            InputStream in = null; // load your input data
            // try-with-resources will also close the input stream
            try (CloseableIterator<SimpleFeature> iter = converter.process(in, context)) {
                while (iter.hasNext()) {
                    iter.next(); // do something with the conversion result
                }
            }
        }
