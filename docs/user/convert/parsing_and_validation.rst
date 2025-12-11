.. _converter_validation:

Parsing and Validation
----------------------

The converter framework provides different options for controlling parsing and validation behavior.

File Encoding
~~~~~~~~~~~~~

Input file encoding can be specified through the ``options.encoding`` key. Encoding must be a valid Java Charset, for
example::

    geomesa.converters.myconverter = {
      options = {
        encoding = "UTF-8"
      }
    }

Validators
~~~~~~~~~~

Validators can check SimpleFeatures before they are written to GeoMesa. For example, you may want to validate
that there is a geometry field and that the geometry is valid, since trying to write a null geometry will usually
result in an error.

There are four validators provided by default:

=========== ===================================================================================================
Name        Description
=========== ===================================================================================================
``index``   Validates that the SimpleFeature has a geometry and date that are within the space/time bounds of
            the relevant GeoMesa Z-Index implementations (i.e. Z2, Z3, XZ2, XZ3)
``has-geo`` Validates that the SimpleFeature has a non-null geometry
``has-dtg`` Validates that the SimpleFeature has a non-null date
``cql``     Validates that the SimpleFeature passes an arbitrary CQL filter
=========== ===================================================================================================

By default the ``index`` validator is enabled. This is suitable for most use cases, as it will choose the appropriate
validator based on the SimpleFeatureType. To enable other validators, specify them in the ``options`` block of your
converter definition::

    geomesa.converters.myconverter = {
      options = {
        validators = [ "has-dtg", "cql(bbox(geom,-75,-90,-45,90))" ]
      }
    }

Validation can be disabled by setting it to an empty array.

Custom Validators
^^^^^^^^^^^^^^^^^

If the provided validators are not sufficient, custom validators may be loaded through Java SPI by implementing and including a
special service descriptor file.

.. raw:: html

   <details>
   <summary><b>Click here for details on custom validators</b></summary>

Custom validators may be loaded through Java SPI by by implementing
``org.locationtech.geomesa.convert2.validators.SimpleFeatureValidatorFactory``, shown below. Note that validators
must be registered through a special
`service descriptor file <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__.

.. code-block:: scala

    trait SimpleFeatureValidatorFactory {

      /**
        * Well-known name of this validator, for specifying the validator to use
        *
        * @return
        */
      def name: String

      /**
        * Create a validator for the given feature typ
        *
        * @param sft simple feature type
        * @param config optional configuration string
        */
      def apply(sft: SimpleFeatureType, config: Option[String]): SimpleFeatureValidator
    }

When specifying validators in a converter config, the ``name`` of the factory must match the ``validators`` string.
Any additional arguments may be specified in parentheses, which will be passed to the ``validator`` method.
For example::

    geomesa.converters.myconverter = {
      options = {
        validators = [ "my-custom-validator(optionA,optionB)" ]
      }
    }

.. code-block:: scala

  import org.locationtech.geomesa.convert2.validators.SimpleFeatureValidatorFactory

  class MyCustomValidator extends SimpleFeatureValidatorFactory {

    override val name: String = "my-custom-validator"

    override def apply(sft: SimpleFeatureType, config: Option[String], tags: Tags): SimpleFeatureValidator = {
      if (config.exists(_.contains("optionA"))) {
        // handle option a
      } else {
        // handle other options
      }
    }
  }

See the GeoMesa
`unit tests <https://github.com/locationtech/geomesa/blob/main/geomesa-convert/geomesa-convert-common/src/test/scala/org/locationtech/geomesa/convert2/validators/SimpleFeatureValidatorTest.scala>`__
for a sample implementation.

.. raw:: html

   </details>

Error Mode
~~~~~~~~~~

Error mode defines how the converter will handle failures during parsing or transforms:

================= ===================================================================================================
Name              Description
================= ===================================================================================================
``log-errors``    The default mode - errors are logged and the converter continues
``raise-errors``  Errors are thrown as exceptions and the conversion process stops. This can be especially useful
                  when first developing and testing a converter definition.
``return-errors`` Errors are exposed through the evaluation context. Generally this is only useful when using
                  converters programmatically.
================= ===================================================================================================

Validation and error messages are logged to the package ``org.locationtech.geomesa.convert``. The field that failed will be
logged at the ``info`` level, while the entire record and stack trace will be logged at the ``debug`` level.

To configure the error mode add the following option to your converter definition:

::

    geomesa.converters.myconverter = {
      options = {
        error-mode = "raise-errors"
      }
    }


Parse Mode
~~~~~~~~~~

Parse mode allows you to control whether data is parsed incrementally or all at once:

=============== ===================================================================================================
Name            Description
=============== ===================================================================================================
``incremental`` The default mode - data is parsed incrementally, providing a stream of results.
``batch``       Data is parsed fully before returning any results. Batch mode can be used to prevent a file from
                being partially ingested if an error occurs. However, it requires greater overhead as the entire
                input must be stored in memory at once.
=============== ===================================================================================================

To configure the parse mode add the following option to your converter definition:

::

    geomesa.converters.myconverter = {
      options = {
        parse-mode = "incremental"
      }
    }
