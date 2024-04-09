GeoTools Overview
=================

The main abstraction in GeoMesa is the GeoTools ``DataStore``. Understanding the GeoTools API is important
to integrating with GeoMesa. The full GeoTools documentation is available `here <https://docs.geotools.org/>`__,
but this section gives a concise overview of the main ways to interact with a data store.

.. note::

    This section is focused on users who want to integrate with GeoMesa through code. Many use cases do not
    require this; data can be ingested using the GeoMesa command-line tools or Apache NiFi processors, and
    accessed through GeoServer OGC requests or Spark. Even so, this page can provide useful background
    on the concepts behind those operations.

A data store provides read and write access to spatial data. The API itself does not distinguish between different
storage formats. Thus, the API for accessing data stored in a local shape file will be the same as for accessing
data stored in an HBase cluster. GeoMesa provides several different data stores implementations, including HBase,
Accumulo, Kafka, and others. See :ref:`geomesa_data_stores` for more information on the different data stores
available.

SimpleFeatureType and SimpleFeature
-----------------------------------

In GeoTools, a ``SimpleFeatureType`` defines the names and types of the attributes in a given schema. It is similar
to the table definition of a relational database. ``SimpleFeatureType``\ s can be described with a type name and a
specification, typically a string indicating the attributes names and types. A ``SimpleFeature`` is a struct data
type, equivalent to a single row in a relational database table. Each ``SimpleFeature`` is associated with a
``SimpleFeatureType``, and has a unique identifier (the feature ID) and a list of values corresponding to the
attributes in the ``SimpleFeatureType``. See below for examples of creating and managing ``SimpleFeatureType``\ s.

The "simple" in ``SimpleFeatureType`` refers to its flat data structure. It is also possible to have "complex"
feature types, which are similar to joins in a relational database. However, complex feature types are not widely
used or supported.

Getting a Data Store Instance
-----------------------------

Data stores are accessed through ``org.geotools.api.data.DataStoreFinder#getDataStore``. The function takes a parameter
map, which is used to dynamically load a data store. For example, to load a GeoMesa HBase data store include the
parameter key ``"hbase.catalog"``. Data stores are dynamically loaded; the appropriate data store implementation
and all of its required dependencies must be on the classpath.

GeoMesa data stores are thread-safe (although not all methods on the data store return thread-safe objects).
Generally, a data store should be loaded once and then used repeatedly. When no longer needed, a data store
should be cleaned up by calling the ``dispose()`` method.

See the links in :ref:`geomesa_data_stores` for an explanation of the parameters for each data store implementation.


.. tabs::

    .. code-tab:: java

        import org.geotools.api.data.DataStore;
        import org.geotools.api.data.DataStoreFinder;
        import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams;

        Map<String, String> parameters = new HashMap<>();
        // HBaseDataStoreParams.HBaseCatalogParam().key is the string "hbase.catalog"
        // the GeoMesa HBase data store will recognize the key and attempt to load itself
        parameters.put(HBaseDataStoreParams.HBaseCatalogParam().key, "mycatalog");
        DataStore store = null;
        try {
            store = DataStoreFinder.getDataStore(parameters);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // when finished, be sure to clean up the store
        if (store != null) {
            store.dispose();
        }

    .. code-tab:: scala

        import org.geotools.api.data.DataStoreFinder
        import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams

        import scala.collection.JavaConverters._

        // HBaseDataStoreParams.HBaseCatalogParam.key is the string "hbase.catalog"
        // the GeoMesa HBase data store will recognize the key and attempt to load itself
        val params = Map(HBaseDataStoreParams.HBaseCatalogParam.key -> "mycatalog")
        val store = DataStoreFinder.getDataStore(params.asJava)
        // when finished, be sure to clean up the store
        store.dispose()

Creating a Schema
-----------------

Each data store can contain multiple ``SimpleFeatureType``\ s, or schemas. Existing schemas can be listed with
the ``getTypeNames`` and ``getSchema`` methods. Schemas can be created, updated and deleted through the
``createSchema``, ``updateSchema`` and ``removeSchema`` methods, respectively.

See :ref:`attribute_types` for a list of the attribute type bindings available.

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
        import org.geotools.api.feature.simple.SimpleFeatureType;

        try {
            String[] types = store.getTypeNames();
            boolean exists = false;
            for (String type: types) {
                if (type.equals("purchases")) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                SimpleFeatureType myType =
                      SimpleFeatureTypes.createType(
                            "purchases", "item:String,amount:Double,date:Date,location:Point:srid=4326");
                store.createSchema(myType);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    .. code-tab:: scala

        import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

        if (!store.getTypeNames.contains("purchases")) {
          val myType =
              SimpleFeatureTypes.createType(
                "purchases", "item:String,amount:Double,date:Date,location:Point:srid=4326")
          store.createSchema(myType)
        }


Writing Data
------------

Data stores support writing data on a row-by-row basis. There are two different write paths - appending writes and
modifying writes.

.. warning::

  Pay close attention to the use of ``PROVIDED_FID`` in the following sections. This hint controls the behavior
  of each feature ID.

Some data stores support transactions, which can be used to isolate a group of operations. GeoMesa does not
support transactions, so the default GeoTools ``Transaction.AUTO_COMMIT`` is used in the examples. Generally,
once a writer is successfully closed, the data has been persisted to the underlying store. Until then,
data may be cached and buffered locally, and may not be persisted or available to query.

Appending Writes
^^^^^^^^^^^^^^^^

An appending writer can be obtained through the ``getFeatureWriterAppend`` method. A feature writer is similar to
an iterator; ``next`` is called to obtain a new feature, the feature is updated with the values to be written,
and then ``write`` is called to persist it. Once all writes are complete, the feature writer should be closed.

The ID used to uniquely identify a feature is called the feature ID, or ``FID``. By default, GeoTools will
generate a new feature ID for each feature. To specify a feature ID, set the ``PROVIDED_FID`` hint in the feature
user data, as shown below.

.. warning::

  It is a logical error to write the same feature ID more than once with an appending feature writer. This
  may result in inconsistencies in the persisted data. Refer to the next section for how to safely update existing
  features.

.. tabs::

    .. code-tab:: java

        import org.geotools.api.data.FeatureWriter;
        import org.geotools.api.data.Transaction;
        import org.geotools.api.feature.simple.SimpleFeature;
        import org.geotools.api.feature.simple.SimpleFeatureType;
        import org.geotools.util.factory.Hints;

        // use try-with-resources to close the writer when done
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                  store.getFeatureWriterAppend("purchases", Transaction.AUTO_COMMIT)) {
            // repeat as needed, once per feature
            // note: hasNext() will always return false, but can be ignored
            SimpleFeature next = writer.next();
            next.getUserData().put(Hints.PROVIDED_FID, "id-01");
            next.setAttribute("item", "swag");
            next.setAttribute("amount", 20.0);
            // attributes will be converted to the appropriate type if needed
            next.setAttribute("date", "2020-01-01T00:00:00.000Z");
            next.setAttribute("location", "POINT (-82.379 34.1782)");
            writer.write();
        } catch (IOException e) {
            e.printStackTrace();
        }

    .. code-tab:: scala

          import org.geotools.util.factory.Hints

          val writer = store.getFeatureWriterAppend("purchases", Transaction.AUTO_COMMIT)
          try {
            // repeat as needed, once per feature
            // note: hasNext will always return false, but can be ignored
            val next = writer.next()
            next.getUserData.put(Hints.PROVIDED_FID, "id-01")
            next.setAttribute("item", "swag")
            next.setAttribute("amount", 20.0)
            // attributes will be converted to the appropriate type if needed
            next.setAttribute("date", "2020-01-01T00:00:00.000Z")
            next.setAttribute("location", "POINT (-82.379 34.1782)")
            writer.write()
          } finally {
            writer.close()
          }

An alternative way to make appending writes is to use a ``FeatureStore``. GeoTools defines a ``FeatureSource`` as
read-only. ``FeatureStore`` extends ``FeatureSource`` and provides write functionality, but must be checked with
a runtime cast.

.. tabs::

    .. code-tab:: java

        import org.geotools.data.simple.SimpleFeatureCollection;
        import org.geotools.api.data.SimpleFeatureSource;
        import org.geotools.api.data.SimpleFeatureStore;
        import org.geotools.feature.DefaultFeatureCollection;

        try {
            SimpleFeatureSource source = store.getFeatureSource("purchases");
            if (source instanceof SimpleFeatureStore) {
                SimpleFeatureCollection collection = new DefaultFeatureCollection();
                // omitted - add features to the collection
                ((SimpleFeatureStore) source).addFeatures(collection);
            } else {
                throw new IllegalStateException("Store is read only");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    .. code-tab:: scala

          import org.geotools.api.data.SimpleFeatureStore
          import org.geotools.feature.DefaultFeatureCollection

          store.getFeatureSource("purchases") match {
            case s: SimpleFeatureStore =>
              val collection = new DefaultFeatureCollection()
              collection.add(???)
              s.addFeatures(collection)

            case _ => throw new IllegalStateException("Store is read only")
          }

Modifying Writes
^^^^^^^^^^^^^^^^

In order to update an existing feature, a modifying writer must be used through the method ``getFeatureWriter``,
which requires a filter specifying the features to be updated. A modifying feature writer is similar to an
appending feature writer, except that the method ``hasNext`` will return ``true`` as long as there are additional
features to modify. The features returned from ``next`` will be pre-populated with the current data for each feature.

Filters can be created through the GeoTools method ``ECQL.toFilter``. See the GeoTools
`documentation <https://docs.geotools.org/stable/userguide/library/cql/ecql.html>`__ for more information
on CQL filters.

.. tabs::

    .. code-tab:: java

        import org.geotools.api.data.FeatureWriter;
        import org.geotools.api.data.Transaction;
        import org.geotools.api.feature.simple.SimpleFeature;
        import org.geotools.api.feature.simple.SimpleFeatureType;
        import org.geotools.filter.text.cql2.CQLException;
        import org.geotools.filter.text.ecql.ECQL;

        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                     store.getFeatureWriter("purchases", ECQL.toFilter("IN ('id-01')"), Transaction.AUTO_COMMIT)) {
            while (writer.hasNext()) {
                SimpleFeature next = writer.next();
                next.setAttribute("amount", 21.0);
                writer.write(); // or, to delete it: writer.remove();
            }
        } catch (IOException | CQLException e) {
            e.printStackTrace();
        }

    .. code-tab:: scala

        import org.geotools.api.data.Transaction
        import org.geotools.filter.text.ecql.ECQL

        val filter = ECQL.toFilter("IN ('id-01')")
        val writer = store.getFeatureWriter("purchases", filter, Transaction.AUTO_COMMIT)
        try {
          while (writer.hasNext) {
            val next = writer.next
            next.setAttribute("amount", 21.0)
            writer.write() // or, to delete it: writer.remove()
          }
        } finally {
          writer.close()
        }

Reading Data
------------

Once data has been persisted, it can be read back through the ``getFeatureReader`` method. GeoTools returns a "live"
iterator of results that may point to a remote location. Generally data is not actually read from the backing store
until it is required, so it is possible to read a few records without fetching the entire result set.

To filter the results that come back, predicates can be created using the "common query language", CQL. Filters can
be created through the GeoTools method ``ECQL.toFilter``. See the GeoTools
`documentation <https://docs.geotools.org/stable/userguide/library/cql/ecql.html>`__ for more information
on CQL filters.


.. tabs::

    .. code-tab:: java

        import org.geotools.api.data.FeatureReader;
        import org.geotools.api.data.Query;
        import org.geotools.api.data.Transaction;
        import org.geotools.data.DataUtilities;
        import org.geotools.filter.text.cql2.CQLException;
        import org.geotools.filter.text.ecql.ECQL;
        import org.geotools.api.feature.simple.SimpleFeature;
        import org.geotools.api.feature.simple.SimpleFeatureType;

        try {
            Query query = new Query("purchases", ECQL.toFilter("bbox(location,-85,30,-80,35)"));
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                       store.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                while (reader.hasNext()) {
                    SimpleFeature next = reader.next();
                    System.out.println(DataUtilities.encodeFeature(next));
                }
            }
        } catch (IOException | CQLException e) {
            e.printStackTrace();
        }

    .. code-tab:: scala

        import org.geotools.api.data.{Query, Transaction}
        import org.geotools.data.DataUtilities
        import org.geotools.filter.text.ecql.ECQL

        val query = new Query("purchases", ECQL.toFilter("bbox(location,-85,30,-80,35)"))
        val reader = store.getFeatureReader(query, Transaction.AUTO_COMMIT)
        try {
          while (reader.hasNext) {
            val next = reader.next
            println(DataUtilities.encodeFeature(next))
          }
        } finally {
          reader.close()
        }
