.. _fsds_index_config:

Index Configuration
===================

When creating a new feature type using the GeoMesa FileSystem data store (FSDS), there are several required options
that must be specified. Through the command line tools, these options are exposed as flags. If using the GeoTools
data store API, the options must be set as user data before calling ``createSchema``.

Configuring the File Encoding
-----------------------------

The FSDS currently supports three options for file encoding: ``orc``, ``parquet``, and ``converter``. Both ORC and
Parquet support reading and writing, while ``converter`` is a synthetic, read-only format that supports querying
a wide variety of data files using the :ref:`converters` API.

File encoding can be specified through the user data key ``geomesa.fs.encoding``:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils;

        SimpleFeatureType sft = ...
        // use the utility method
        ConfigurationUtils.setEncoding(sft, "parquet");
        // or set directly in the user data
        sft.getUserData().put("geomesa.fs.encoding", "parquet");

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

        val sft: SimpleFeatureType = ???
        // use the implicit method from RichSimpleFeatureType
        sft.setEncoding("parquet")
        // or set directly in the user data
        sft.getUserData.put("geomesa.fs.encoding", "parquet");

.. _partition_scheme_config:

Configuring the Partition Scheme
--------------------------------

Partition schemes define how data files are laid out in folders on the file system. Schemes are defined by a
well-known name, and an optional map of configuration values. See :ref:`fsds_partition_schemes` for more details.

Partition schemes can be specified through the user data key ``geomesa.fs.scheme``:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils;
        import java.util.Collections;

        SimpleFeatureType sft = ...
        // use the utility method
        ConfigurationUtils.setScheme(sft, "daily", Collections.singletonMap("dtg-attribute", "dtg"));
        // or set directly in the user data as JSON
        sft.getUserData().put("geomesa.fs.scheme",
            "{ \"name\": \"daily\", \"options\": { \"dtg-attribute\": \"dtg\" } }");

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

        val sft: SimpleFeatureType = ???
        // use the implicit method from RichSimpleFeatureType
        sft.setScheme("daily", Map("dtg-attribute" -> "dtg"))
        // or set directly in the user data
        sft.getUserData.put("geomesa.fs.scheme",
            """{ "name": "daily", "options": { "dtg-attribute": "dtg" } }""")

Configuring Leaf Storage
------------------------

Leaf storage controls the final layout of files and folders. When using leaf storage (which is enabled by default),
the last component of the partition path is used as a prefix to the data file name, instead of as a separate folder.
This can result in less directory overhead for filesystems such as S3.

As an example, a partition scheme of ``yyyy/MM/dd`` would produce a partition path like ``2016/01/01``. With
leaf storage, the data files for that partition would be ``2016/01/01_<datafile>.parquet``. If leaf storage is
disabled, the data files would be ``2016/01/01/<datafile>.parquet``, creating an extra level of directories.

Leaf storage can be specified through the user data key ``geomesa.fs.leaf-storage``:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils;

        SimpleFeatureType sft = ...
        // use the utility method
        ConfigurationUtils.setLeafStorage(sft, false);
        // or set directly in the user data as a string
        sft.getUserData().put("geomesa.fs.leaf-storage", "false");

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

        val sft: SimpleFeatureType = ???
        // use the implicit method from RichSimpleFeatureType
        sft.setLeafStorage(false)
        // or set directly in the user data as a string
        sft.getUserData.put("geomesa.fs.leaf-storage", "false")

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

.. _fsds_metadata_config:

Configuring Metadata Persistence
--------------------------------

The FSDS keeps metadata on partitions and data files, to avoid repeatedly interrogating the file system. By default,
metadata information is stored as a change log in the file system, which does not require any additional
infrastructure. For more advanced use-cases, the FSDS also supports persisting metadata in a relational database
using JDBC. For more information, see :ref:`fsds_metadata`.

Metadata persistence can be specified through the user data key ``geomesa.fs.metadata``:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils;
        import java.util.Collections;

        SimpleFeatureType sft = ...
        // use the utility method
        Map<String, String> options = Collections.singletonMap("jdbc.url", "jdbc:postgresql://localhost/geomesa");
        ConfigurationUtils.setMetadata(sft, "jdbc", options);
        // or set directly in the user data as JSON
        sft.getUserData().put("geomesa.fs.metadata",
            "{ \"name\": \"jdbc\", \"options\": { \"jdbc.url\": \"jdbc:postgresql://localhost/geomesa\" } }");

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

        val sft: SimpleFeatureType = ???
        // use the implicit method from RichSimpleFeatureType
        sft.setMetadata("jdbc", Map("jdbc.url" -> "jdbc:postgresql://localhost/geomesa"))
        // or set directly in the user data as JSON
        sft.getUserData.put("geomesa.fs.metadata",
            """{ "name": "jdbc", "options": { "jdbc.url": "jdbc:postgresql://localhost/geomesa" } }""")

.. note::

  The metadata configuration supports property substitution using environment variables and Java system
  properties. Property substitutions are specified using ``${}`` syntax, e.g. ``${HOME}`` or ``${user.home}``.

Configuring Custom Observer Callbacks
-------------------------------------

The FSDS provides a mechanism to add custom handling during file writing. Users can implement observer factories,
which will be invoked for each new file that is created. Observer factories must extend the trait
``FileSystemObserverFactory``:

.. code-block:: scala

  package org.locationtech.geomesa.fs.storage.common.observer

  trait FileSystemObserverFactory extends Closeable {

    /**
     * Called once after instantiating the factory
     *
     * @param conf hadoop configuration
     * @param root root path
     * @param sft simple feature type
     */
    def init(conf: Configuration, root: Path, sft: SimpleFeatureType): Unit

    /**
     * Create an observer for the given path
     *
     * @param path file path being written
     * @return
     */
    def apply(path: Path): FileSystemObserver
  }

.. note::

  Observer factories must have a default no-arg constructor in order to be instantiated by the framework.

Observers can be specified through the user data key ``geomesa.fs.observers``:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils;
        import java.util.Arrays;
        import java.util.Collections;
        import java.util.List;

        SimpleFeatureType sft = ...
        List<String> factories =
          Arrays.asList("com.example.MyCustomObserverFactory", "com.example.MySecondObserverFactory");
        // use the static utility method
        ConfigurationUtils.setObservers(sft, factories);
        // or set directly in the user data as a comma-delimited string
        sft.getUserData().put("geomesa.fs.observers", String.join(",", factories));

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

        val sft: SimpleFeatureType = ???
        val factories = Seq("com.example.MyCustomObserverFactory", "com.example.MySecondObserverFactory")
        // use the implicit method from RichSimpleFeatureType
        sft.setObservers(factories)
        // or set directly in the user data as a comma-delimited string
        sft.getUserData.put("geomesa.fs.observers", factories.mkString(","))

Configuring Path Filters
------------------------

.. note::

  Path filtering is supported for ``converter`` encoding only.

The FSDS can filter paths within a partition for more granular control of queries. Path filtering is configured
through the user data key ``geomesa.fs.path-filter.name``.

Currently, the only implementation is the ``dtg`` path filter, whose purpose is to parse a datetime from the given
path and compare it to the query filter to include or exclude the file from the query. The following options are
required for the ``dtg`` path filter, configured through the key ``geomesa.fs.path-filter.opts``:

* ``attribute`` - The ``Date`` attribute in the query to compare against.
* ``pattern`` - The regular expression, with a single capturing group, to extract a datetime string from the path.
* ``format`` - The datetime formatting pattern to parse a date from the regex capture.
* ``buffer`` - The duration to buffer the bounds of the parsed datetime by within the current partition. To buffer time
  across partitions, see the ``receipt-time`` partition scheme.

Custom path filters can be loaded via SPI.
