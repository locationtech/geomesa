.. _fsds_transactions:

Transactions
============

The FileSystem data store supports atomic writes through the GeoTools `transaction API`_. All of the appends,
updates and deletes made in a transaction are published in a single Iceberg commit, so readers will either see
all of the changes or none of them.

.. tabs::

    .. code-tab:: java

        import org.geotools.api.data.DataStore;
        import org.geotools.api.data.DataStoreFinder;
        import org.geotools.api.data.FeatureWriter;
        import org.geotools.api.data.Transaction;
        import org.geotools.data.DefaultTransaction;
        import org.geotools.filter.text.ecql.ECQL;

        DataStore store = DataStoreFinder.getDataStore(params);
        // closing the transaction will discard any changes that haven't been committed
        try (Transaction transaction = new DefaultTransaction()) {
            try {
                // pass the transaction when getting a feature writer
                try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                            store.getFeatureWriterAppend("my-type", transaction)) {
                    // write some features (elided)
                }
                try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                            store.getFeatureWriter("my-type", ECQL.toFilter("IN ('1', '2')"), transaction)) {
                    // modify or remove some features (elided)
                }
                // writers must be closed before committing the transaction
                transaction.commit();
            } catch (IOException e) {
                // discards the changes and deletes any files that were written
                transaction.rollback();
            }
        }

    .. code-tab:: scala

        import org.geotools.api.data.DataStoreFinder
        import org.geotools.data.DefaultTransaction
        import org.geotools.filter.text.ecql.ECQL

        import java.io.IOException

        val store = DataStoreFinder.getDataStore(params)
        // closing the transaction will discard any changes that haven't been committed
        val transaction = new DefaultTransaction()
        try {
          try {
            // pass the transaction when getting a feature writer
            val appender = store.getFeatureWriterAppend("my-type", transaction)
            try {
              // write some features (elided)
            } finally {
              appender.close()
            }
            val modifier = store.getFeatureWriter("my-type", ECQL.toFilter("IN ('1', '2')"), transaction)
            try {
              // modify or remove some features (elided)
            } finally {
              modifier.close()
            }
            // writers must be closed before committing the transaction
            transaction.commit()
          } catch {
            case _: IOException =>
              // discards the changes and deletes any files that were written
              transaction.rollback()
          }
        } finally {
          transaction.close()
        }

As with any GeoTools transaction, a transaction may be re-used after it has been committed or rolled back, until
it is closed.

Auto-Commit
-----------

When using ``Transaction.AUTO_COMMIT``, each feature writer behaves as its own transaction, which is committed when
the writer is closed. Data is not visible until then, regardless of how many files have been written. If a writer
fails, none of the data it has written will be persisted.

Isolation
---------

Changes made in a transaction are not visible until the transaction is committed, including to readers and writers
that are using the same transaction. As a consequence, a feature that was added in a transaction can't be modified
in the same transaction, and an existing feature can only be modified once per transaction.

Conflicts
---------

Appending writes never conflict with other operations. Modifying writes will conflict if another operation has
modified or removed the same features since they were read, or has re-written the files containing them (for
example, a compaction). Modifying the same feature more than once in a transaction is also a conflict.

Conflicts are reported with a ``ConflictException``, which is a type of ``IOException``. It may be thrown when:

* committing a transaction
* closing a feature writer that is using ``Transaction.AUTO_COMMIT``
* modifying or removing a feature that has already been modified in the same transaction

After a conflict, the transaction should be rolled back, and may be re-tried:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.fs.storage.core.StorageTransaction.ConflictException;

        try {
            // write, modify or remove some features (elided)
            transaction.commit();
        } catch (ConflictException e) {
            // discard the changes, and then re-try them if desired
            transaction.rollback();
        } catch (IOException e) {
            // some other failure
            transaction.rollback();
        }

    .. code-tab:: scala

        import org.locationtech.geomesa.fs.storage.core.StorageTransaction.ConflictException

        try {
          // write, modify or remove some features (elided)
          transaction.commit()
        } catch {
          case _: ConflictException =>
            // discard the changes, and then re-try them if desired
            transaction.rollback()
          case _: IOException =>
            // some other failure
            transaction.rollback()
        }

Limitations
-----------

* A transaction that is used with more than one feature type is committed separately for each feature type. Each
  commit is atomic, but the transaction as a whole is not - if one commit fails, the others may still succeed.
  If any of the commits fail due to a conflict, a ``ConflictException`` is thrown, with any other failures attached
  as suppressed exceptions. Feature types that were committed successfully are not re-committed if the transaction
  is re-tried. If the transaction spans multiple data stores and more than one of them fails, GeoTools will wrap
  the failure in a ``DataSourceException``.
* If a process exits before a transaction is committed or rolled back, the files written by the transaction will
  be left in place. As they are not referenced by the table they will not affect queries, and can be removed by
  Iceberg's orphan file clean-up.

.. _transaction API: https://docs.geotools.org/stable/javadocs/org/geotools/api/data/Transaction.html
