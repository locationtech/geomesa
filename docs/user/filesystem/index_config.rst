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

Configuring Structural Types
----------------------------

The FileSystem data store supports the concept of "structural types" - these are JSON fields with a defined schema, which means
they can be efficiently persisted and queried on disk. Structural types help bridge the gap between GeoTools simple feature types
and complex feature types that support nested and repeated structures.

To specify an attribute as a structural types, use ``json=true`` along with the ``json-schema`` option, which must be an
`Avro schema definition <https://avro.apache.org/docs/1.12.0/specification/#schema-declaration>`__:

.. tabs::

    .. code-tab:: java

        DataStore ds = ...
        SimpleFeatureType sft = SimpleFeatureTypes.createType("test", "ids:String:json=true,dtg:Date,*geom:Point:srid=4326");
        String schema = """
                        {
                          "type": "array",
                          "items": {
                            "type": "record",
                            "name": "item",
                            "fields": [
                              { "name": "id", "type": "int" },
                              { "name": "label", "type": ["null", "string"], "default": null }
                            ]
                          }
                        }""".replaceAll("[\n ]", "");
        sft.getDescriptor("ids").getUserData().put("json-schema", schema);
        ds.createSchema(sft);

        try (var writer = ds.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
            var feature = writer.next();
            feature.setAttribute("ids", "[{\"id\":1,\"label\":\"a\"},{\"id\":2,\"label\":\"b\"}]");
            feature.setAttribute("dtg", "2026-01-01T00:00:01.000Z");
            feature.setAttribute("geom", "POINT(40 50)");
            writer.write();
        }

    .. code-tab:: scala

        val ds: DataStore = ???
        val sft = SimpleFeatureTypes.createType("test", "ids:String:json=true,dtg:Date,*geom:Point:srid=4326")
        val schema =
          """{
            |  "type": "array",
            |  "items": {
            |    "type": "record",
            |    "name": "item",
            |    "fields": [
            |      { "name": "id", "type": "int" },
            |      { "name": "label", "type": ["null", "string"], "default": null }
            |    ]
            |  }
            |}""".stripMargin.replaceAll("[\n ]", "")
        sft.getDescriptor("ids").getUserData.put("json-schema", schema)
        ds.createSchema(sft)

        val writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        try {
          val feature = writer.next()
          feature.setAttribute("ids", """[{"id":1,"label":"a"},{"id":2,"label":"b"}]""")
          feature.setAttribute("dtg", "2026-01-01T00:00:01.000Z")
          feature.setAttribute("geom", "POINT(40 50)")
          writer.write()
        } finally {
          writer.close()
        }

The top-level element in the Avro schema must be a record, array or map type.

.. warning::

    Any values that do not match the structural type schema will not be persisted.

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

Once the schema has been created, the file size can be configured through the storage metadata key ``target-file-size``. See
:ref:`fsds_manage_metadata_command` for setting metadata keys, and see :ref:`fsds_size_threshold_prop` for controlling the file
size error margin.

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
