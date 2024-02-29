Atomic Write Operations
=======================

Accumulo supports atomic read-modify-write operations through
`conditional operations <https://accumulo.apache.org/docs/2.x/getting-started/clients#conditionalwriter>`__.
Conditional operations can be used when the ordering of writes is important - it guarantees that all updates
transition between known states.

GeoMesa supports atomics writes through the GeoTools API, and through a lower-level custom API that can be more
efficient.

.. warning::

    In order for atomicity to be enforced, **all** writes for a given feature must be made through an atomic
    writer. Regular writers do not respect row-level locks, and can break atomic operations.

.. warning::

    GeoMesa writes to multiple rows and tables for any given feature. Each row-level operation is atomic, but
    operations across multiple rows and tables are not. Even when using atomic writers, index tables may be left
    in an inconsistent state in some rare cases. For example, if multiple writers are trying to update a single
    feature at the same time that a tablet server crashes, some failed operations may not be rolled back successfully.

Using an Atomic Writer
----------------------

To use an atomic writer through the GeoTools API, just pass in a special ``AtomicWriteTransaction``:

.. tabs::

    .. code-tab:: java

        import org.geotools.api.data.DataStore;
        import org.geotools.api.data.FeatureWriter;
        import org.geotools.api.feature.simple.SimpleFeature;
        import org.geotools.api.feature.simple.SimpleFeatureType;
        import org.geotools.filter.text.ecql.ECQL;
        import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException;
        import org.locationtech.geomesa.index.geotools.AtomicWriteTransaction;

        DataStore ds = ...;
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                       ds.getFeatureWriter("mysft",
                                            ECQL.toFilter("IN('myid')"),
                                            AtomicWriteTransaction.INSTANCE)) {
            while (writer.hasNext()) {
                SimpleFeature next = writer.next();
                // apply updates, then:
                writer.write();
            }
        } catch (ConditionalWriteException e) {
            // handle failures by retrying, altering your updates as needed based on the new feature data
        }

    .. code-tab:: scala

        import org.geotools.api.data.DataStore
        import org.geotools.filter.text.ecql.ECQL
        import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException
        import org.locationtech.geomesa.index.geotools.AtomicWriteTransaction

        val ds: DataStore = ???;
        val writer = ds.getFeatureWriter("mysft", ECQL.toFilter("IN('myid')"), AtomicWriteTransaction.INSTANCE)
        try {
          while (writer.hasNext()) {
            val next = writer.next()
            // apply updates, then:
            writer.write()
          }
        } catch {
          case e: ConditionalWriteException =>
            // handle failures by retrying, altering your updates as needed based on the new feature data
        } finally {
          writer.close()
        }

Alternatively, use the lower-level GeoMesa API, which avoids having to query Accumulo for each update, assuming
the feature state is readily available:

.. tabs::

    .. code-tab:: java

        import org.geotools.api.data.DataStore;
        import org.geotools.api.feature.simple.SimpleFeature;
        import org.geotools.api.feature.simple.SimpleFeatureType;
        import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
        import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException;
        import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter;
        import org.locationtech.geomesa.utils.index.IndexMode;
        import scala.Option;

        AccumuloDataStore ds = (AccumuloDataStore) ...;
        SimpleFeatureType sft = ds.getSchema("mysft");
        try (IndexWriter writer = ds.adapter().createWriter(sft,
                                                            ds.manager().indices(sft, IndexMode.Write()),
                                                            Option.empty(),
                                                            true)) {
            SimpleFeature current = ...;
            SimpleFeature update = ...;
            try {
                writer.update(update, current);
            } catch (ConditionalWriteException e) {
                // handle failures by retrying, altering your updates as needed based on the new feature data
            }
        }

    .. code-tab:: scala

        import org.geotools.api.feature.simple.SimpleFeature
        import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
        import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException
        import org.locationtech.geomesa.utils.index.IndexMode

        val ds: AccumuloDataStore = ???
        val sft = ds.getSchema("mysft")
        val indices = ds.manager.indices(sft, mode = IndexMode.Write)
        val writer = ds.adapter.createWriter(sft, indices, None, atomic = true)
        try {
          try {
            val current: SimpleFeature = ???
            val update: SimpleFeature = ???
            writer.update(update, current)
          } catch {
            case e: ConditionalWriteException =>
              // handle failures by retrying, altering your updates as needed based on the new feature data
          }
        } finally {
          writer.close()
        }
