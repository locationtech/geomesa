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
