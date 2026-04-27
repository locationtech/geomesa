.. _fsds_index_config:

Index Configuration
===================

When creating a new feature type using the GeoMesa FileSystem data store (FSDS), there are several required options
that must be specified. Through the command line tools, these options are exposed as flags. If using the GeoTools
data store API, the options must be set as user data before calling ``createSchema``.

.. _partition_scheme_config:

Configuring the Partition Scheme
--------------------------------

Partition schemes define how data is partitioned (grouped) into different files. Schemes are defined by a
well-known name, along with optional configuration flags. See :ref:`fsds_partition_schemes` for more details.

Partition schemes can be specified through the user data key ``geomesa.fs.scheme``:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils;
        import java.util.Collections;

        SimpleFeatureType sft = ...
        // use the utility method
        ConfigurationUtils.setScheme(sft, "daily,z2:bits=2");
        // or set directly in the user data as JSON
        sft.getUserData().put("geomesa.fs.scheme", "daily,z2:bits=2");

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

        val sft: SimpleFeatureType = ???
        // use the implicit method from RichSimpleFeatureType
        sft.setScheme("daily,z2:bits=2")
        // or set directly in the user data
        sft.getUserData.put("geomesa.fs.scheme", "daily,z2:bits=2")

Different schemes are separated with a comma (``,``), while scheme options are separated with a colon (``:``), and the
value for each option is separated by an equals sign (``=``). For example:
``first_scheme_name:option.1.key=option1value:option.2.key=option2value,second_scheme_name``

.. _fsds_file_size_config:

Configuring Target File Size
----------------------------

By default data files can grow to unlimited size as more data is written and files are compacted. This may lead
to poor performance, if a file becomes too large. To manage this, a target file size can be configured through
the user data key ``geomesa.fs.file-size``:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils;

        SimpleFeatureType sft = ...
        // use the utility method
        ConfigurationUtils.setTargetFileSize(sft, false);
        // or set directly in the user data as a string
        sft.getUserData().put("geomesa.fs.file-size", "1GB");

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

        val sft: SimpleFeatureType = ???
        // use the implicit method from RichSimpleFeatureType
        sft.setTargetFileSize("1GB")
        // or set directly in the user data as a string
        sft.getUserData.put("geomesa.fs.file-size", "1GB")

Note that target file size can also be specified in some operations, which will override any default configured
in the feature type. See :ref:`fsds_compact_command` and :ref:`fsds_ingest_command` for details. See
:ref:`fsds_size_threshold_prop` for controlling the file size error margin.

Configuring Visibility Persistence
----------------------------------

GeoMesa will by default persist feature visibility flags as a column in the FSDS files. If feature visibilities are not being
used, this may be disabled by settings ``geomesa.fs.visibilities`` to ``false``, either in the feature type user data
or in the ``fs.config.properties`` or ``fs.config.file`` data store parameters.

Configuring Custom Observer Callbacks
-------------------------------------

The FSDS provides a mechanism to add custom handling during file writing. Users can implement observer factories,
which will be invoked for each new file that is created. Observer factories must extend the trait
``org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory``:

.. code-block:: scala

    package org.locationtech.geomesa.fs.storage.core.observer

    import org.locationtech.geomesa.fs.storage.core.FileSystemStorage

    import java.io.Closeable
    import java.net.URI

    /**
     * Factory for observing file writes
     */
    trait FileSystemObserverFactory extends Closeable {

      /**
       * Called once after instantiating the factory
       *
       * @param conf hadoop configuration
       * @param root root path
       * @param sft simple feature type
       */
      def init(storage: FileSystemStorage): Unit

      /**
       * Create an observer for the given path
       *
       * @param path file path being written
       * @return
       */
      def apply(path: URI): FileSystemObserver
    }

.. note::

  Observer factories must have a default no-arg constructor in order to be instantiated by the framework.

Observers can be specified through the user data key ``geomesa.fs.observers``:

.. tabs::

    .. code-tab:: java

        import java.util.Arrays;
        import java.util.Collections;
        import java.util.List;

        SimpleFeatureType sft = ...
        List<String> factories =
          Arrays.asList("com.example.MyCustomObserverFactory", "com.example.MySecondObserverFactory");
        // set directly in the user data as a comma-delimited string
        sft.getUserData().put("geomesa.fs.observers", String.join(",", factories));

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.core.RichSimpleFeatureType

        val sft: SimpleFeatureType = ???
        val factories = Seq("com.example.MyCustomObserverFactory", "com.example.MySecondObserverFactory")
        // use the implicit method from RichSimpleFeatureType
        sft.setObservers(factories)
        // or set directly in the user data as a comma-delimited string
        sft.getUserData.put("geomesa.fs.observers", factories.mkString(","))
