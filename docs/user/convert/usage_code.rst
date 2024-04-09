Using Converters Programmatically
---------------------------------

Converters and SimpleFeatureTypes can be imported through maven and used directly in code::

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <!-- pull in all converters, or use a specific converter, e.g. geomesa-convert-json_2.12 -->
      <artifactId>geomesa-convert-all_2.12</artifactId>
    </dependency>

.. tabs::

    .. code-tab:: scala

        import org.locationtech.geomesa.convert.ConverterConfigLoader
        import org.locationtech.geomesa.convert2.SimpleFeatureConverter
        import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader

        val sft = SimpleFeatureTypeLoader.sftForName("example-csv").getOrElse {
          throw new RuntimeException("Could not load feature type")
        }
        val conf = ConverterConfigLoader.configForName("example-csv").getOrElse {
          throw new RuntimeException("Could not load converter definition")
        }
        val converter = SimpleFeatureConverter(sft, conf)
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

        import com.typesafe.config.Config;
        import com.typesafe.config.ConfigFactory;
        import org.geotools.api.feature.simple.SimpleFeature;
        import org.geotools.api.feature.simple.SimpleFeatureType;
        import org.locationtech.geomesa.convert.ConverterConfigLoader;
        import org.locationtech.geomesa.convert.EvaluationContext;
        import org.locationtech.geomesa.convert2.SimpleFeatureConverter;
        import org.locationtech.geomesa.convert2.interop.SimpleFeatureConverterLoader;
        import org.locationtech.geomesa.utils.collection.CloseableIterator;
        import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader;

        import java.util.Collections;

        scala.Option<SimpleFeatureType> sftOption = SimpleFeatureTypeLoader.sftForName("example-csv");
        if (sftOption.isEmpty()) {
          throw new RuntimeException("Could not load feature type");
        }
        SimpleFeatureType sft = sftOption.get();

        scala.Option<Config> confOption = ConverterConfigLoader.configForName("example-csv");
        if (confOption.isEmpty()) {
          throw new RuntimeException("Could not load converter definition");
        }
        Config conf = confOption.get();

        // use try-with-resources to clean up the converter when we're done
        try (SimpleFeatureConverter converter = SimpleFeatureConverterLoader.load(sft, conf)) {
            InputStream in = null; // load your input data
            EvaluationContext context = converter.createEvaluationContext(Collections.emptyMap());
            try (CloseableIterator<SimpleFeature> iter = converter.process(in, context)) {
                while (iter.hasNext()) {
                    iter.next(); // do something with the conversion result
                }
            }
        }
