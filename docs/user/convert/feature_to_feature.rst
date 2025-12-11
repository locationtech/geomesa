Feature-To-Feature Converter
============================

The feature-to-feature converter can be used to transform ``SimpleFeature``\ s from one ``SimpleFeatureType`` to another.
Unlike other GeoMesa converters, the feature-to-feature converter must be invoked programmatically, as there is no
native decoding of features from an input stream.

Configuration
-------------

The feature-to-feature converter supports the following configuration keys:

=============== ======== ======= ==========================================================================================
Key             Required Type    Description
=============== ======== ======= ==========================================================================================
``type``        yes      String  Must be the string ``simple-feature``.
``input-sft``   yes      String  Name of a feature type available on the classpath.
=============== ======== ======= ==========================================================================================

``input-sft``
^^^^^^^^^^^^^

The input feature type must be defined with ``input-sft``, which must reference the name of a feature type available on the
classpath - see :ref:`converter_sft_defs` for details on making the feature type available.

Transform Functions
-------------------

The attributes of the input feature type can be accessed by transforms, using ``$`` notation. Note
that the input feature type will take precedence over any converter fields when resolving attribute references. Any
fields that have the same name as the input type will be automatically copied, unless they are explicitly redefined in the
converter definition. The feature ID will also be copied, unless it is redefined with ``id-field``.

Example Usage
-------------

Given an input feature type defined as:

::

    geomesa.sfts.intype = {
      type-name = "intype"
      attributes = [
        { name = "number", type = "Integer" }
        { name = "color",  type = "String"  }
        { name = "weight", type = "Double"  }
        { name = "geom",   type = "Point"   }
      ]
    }

And an output feature type defined as:

::

    geomesa.sfts.outtype = {
      type-name = "outtype"
      attributes = [
        { name = "number",   type = "Integer" }
        { name = "color",    type = "String"  }
        { name = "weight",   type = "Double"  }
        { name = "numberx2", type = "Integer" }
        { name = "geom",     type = "Point"   }
      ]
    }

The following example will copy the attributes of the input features, while adding a new attribute (``numberx2``) derived
from one of the input fields:

::

    geomesa.converters.myconverter = {
      type = "simple-feature"
      input-sft = "intype"
      fields = [
        // note: number, color, weight, and geom will be auto-copied since they exist in both input and output types
        { name = "numberx2", transform = "add($number, $number)::int" }
      ]
    }

.. tabs::

    .. code-tab:: scala

        import org.geotools.api.feature.simple.SimpleFeature
        import org.locationtech.geomesa.convert2.simplefeature.FeatureToFeatureConverter
        import org.locationtech.geomesa.utils.collection.CloseableIterator

        val converter = FeatureToFeatureConverter("outtype", "myconverter")
        try {
          val features: Iterator[SimpleFeature] = ??? // list of input features to transform
          val iter = converter.convert(CloseableIterator(features))
          try {
            iter.foreach(???) // do something with the conversion result
          } finally {
            iter.close()
          }
        } finally {
          converter.close() // clean up any resources associated with your converter
        }

    .. code-tab:: java

        import org.geotools.api.feature.simple.SimpleFeature;
        import org.locationtech.geomesa.convert.EvaluationContext;
        import org.locationtech.geomesa.convert2.simplefeature.FeatureToFeatureConverter;
        import org.locationtech.geomesa.utils.collection.CloseableIterator;

        import java.util.List;
        import java.util.Map;

        List<SimpleFeature> features = ...; // list of input features to transform

        // use try-with-resources to clean up the converter when we're done
        try (FeatureToFeatureConverter converter = FeatureToFeatureConverter.apply("outtype", "myconverter")) {
            EvaluationContext context = converter.createEvaluationContext(Map.of());
            try (CloseableIterator<SimpleFeature> iter = converter.convert(CloseableIterator.apply(features.iterator()), ec)) {
                while (iter.hasNext()) {
                    iter.next(); // do something with the conversion result
                }
            }
        }
