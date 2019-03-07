.. _converter_validation:

Parsing and Validation
----------------------

GeoMesa provides options for dealing with bad input records which can be configured on your converter. For example,
you may want to skip over bad records for some input sources or fail entire files in other cases. The Converter
framework provides a few options that can help control how errors in input data are handled.

Validators
~~~~~~~~~~

At their core, converters transform input streams into SimpleFeatures. Validators provide hooks to validate properties
of those SimpleFeatures before they are written to GeoMesa. For example, you may want to validate that there is a
geometry field and that the geometry is valid.

There are three validators provided for use with GeoMesa converters:

* ``has-geo`` - Ensure that the SimpleFeature has a non-null geometry
* ``has-dtg`` - Ensure that the SimpleFeature has a non-null date
* ``index`` - Ensure that the SimpleFeature has a geometry and date that are within the space/time bounds of
  the relevant GeoMesa Z-Index implementations (i.e. Z2, Z3, XZ2, XZ3)

Additional validators may be loaded through Java SPI by by implementing
``org.locationtech.geomesa.convert.SimpleFeatureValidator$ValidatorFactory`` and including a special service
descriptor file. See below for additional information.

By default the ``index`` validator is enabled. This is suitable for most use cases, as it will choose the appropriate
validator based on the SimpleFeatureType. To enable other validators, specify them in the options block of your
typesafe converter config::

    geomesa.converters.myconverter {
      options {
        validators = [ "has-dtg", "has-geo" ]
      }
    }

Validation can be disabled by setting it to an empty array.

Custom Validators
^^^^^^^^^^^^^^^^^

Custom validators may be loaded through Java SPI by by implementing
``org.locationtech.geomesa.convert.SimpleFeatureValidator$ValidatorFactory``, shown below. Note that validators
must be registered through a special service descriptor file.

.. code-block:: scala

  trait ValidatorFactory {
    def name: String
    def validator(sft: SimpleFeatureType, config: Option[String]): Validator
  }

When specifying validators in a converter config, the ``name`` of the factory must match the ``validators`` string.
Any additional arguments may be specified in parentheses, which will be passed to the ``validator`` method.
For example::

    geomesa.converters.myconverter {
      options {
        validators = [ "my-custom-validator(optionA,optionB)" ]
      }
    }

.. code-block:: scala

  class MyCustomValidator extends ValidatorFactory {

    override val name: String = "my-custom-validator"

    override def validator(sft: SimpleFeatureType, config: Option[String]): Validator = {
      if (config.exists(_.contains("optionA"))) {
        // handle option a
      } else {
        // handle other options
      }
    }
  }

See the GeoMesa
`unit tests <https://github.com/locationtech/geomesa/blob/master/geomesa-convert/geomesa-convert-common/src/test/scala/org/locationtech/geomesa/convert/SimpleFeatureValidatorTest.scala>`__
for a sample implementation.

For more details on implementing a service provider, see the
`Oracle Javadoc <http://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html>`__.

Error Mode
~~~~~~~~~~

There are two types of modes for handling errors:

* ``skip-bad-records``
* ``raise-errors``

``raise-errors`` mode will throw an IOException if bad data is detected based on parsing or validation. This can
be especially useful when first developing and testing a converter definition. ``skip-bad-records`` mode will
still provide debug level logging but will not throw an exception. To configure the
error mode add the following option to your converter's typesafe config:

::

    geomesa.converters.myconverter {
      options {
        error-mode = "raise-errors"
      }
    }


Parse Mode
~~~~~~~~~~

The parse mode option allows you to control whether a file is parsed incrementally or fully before being converted. This
is important when it comes to validation. There are two modes available:

* ``incremental``
* ``batch``

Since converters provide iterators of SimpleFeatures, the default parse mode is ``incremental`` which provides better
performance and less memory overhead. Using ``incremental`` parse mode means that data may be partially ingested into
GeoMesa before an error is raised. In most cases this is appropriate and can be handled by the client code.

Using ``batch`` mode will buffer an entire input stream or file in memory to validate the data which is less performant
and requires more memory. ``batch`` mode does, however, prevent partially ingested data streams. It is unlikely that
you need to use ``batch`` mode in environments where data is properly sanitized.

To configure the parse mode use add an option to your converter's typesafe config:

::

    geomesa.converters.myconverter {
      options {
        parse-mode = "incremental"
      }
    }

Logging
~~~~~~~

To view validation logs you can enable info or debug level logging on the packages
``org.locationtech.geomesa.convert`` and ``org.locationtech.geomesa.convert2``.

When logging is enabled at the info level, it will just show the field that failed. When enabled at the debug
level, it will show the entire record, along with the stack trace.

Transactional Considerations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most of the datastores that GeoMesa works with (Accumulo, HBase, etc) do not provide transactions. Therefore, streaming
data in and out of a converter and into an ingest pipeline is not transactional. To mimic transactions you can use
a batch parse mode with ``raise-errors`` error mode and with the ``index`` validator. Note that this may
increase your memory requirements and hurt performance:

::

    geomesa.converters.myconverter {
      options {
        validators = [ "index" ]
        parse-mode = "batch"
        error-mode = "raise-errors"
      }
    }

If you need notification of bad input data you may consider using an error mode of ``raise-errors`` with an
incremental parse mode:

::

    geomesa.converters.myconverter {
      options {
        validators = [ "index" ]
        parse-mode = "incremental"
        error-mode = "raise-errors"
      }
    }

If you are using a framework such as the GeoMesa Nifi processor, then the file will still be routed to an error
relationship but you may experience partially ingested data. See :doc:`/user/nifi` for more info.

Managing Parsing and Validation Configuration with System Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For inferred converters, one can manage the parsing, line, and validation modes via system property or
``geomesa-site.xml``.  For each of the modes in the table below, the corresponding property name is given.

============== ========================================
Mode           System Property
============== ========================================
Error Mode     ``geomesa.converter.error.mode.default``
Parse Mode     ``geomesa.converter.parse.mode.default``
Line Mode      ``geomesa.converter.line.mode.default``
Validator Mode ``geomesa.converter.validators``
============== ========================================