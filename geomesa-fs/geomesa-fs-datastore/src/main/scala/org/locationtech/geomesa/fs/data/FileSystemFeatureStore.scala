/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{DataStore, FeatureReader, FeatureWriter, Query, QueryCapabilities, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore.FileSystemDataStoreConfig
import org.locationtech.geomesa.fs.data.FileSystemFeatureStore._
import org.locationtech.geomesa.fs.storage.core.StorageTransaction.ConflictException
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, StorageTransaction}
import org.locationtech.geomesa.index.geotools.{FastSettableFeatureWriter, GeoMesaFeatureWriter}
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.json.JsonPathPropertyNameResolver

import java.io.{Closeable, IOException}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class FileSystemFeatureStore(
    val storage: FileSystemStorage,
    entry: ContentEntry,
    query: Query,
    config: FileSystemDataStoreConfig,
  ) extends ContentFeatureStore(entry, query) with LazyLogging {

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    val transaction = storageTransaction()
    // note: check update first as sometimes we get ADD | UPDATE
    if ((flags & WRITER_UPDATE) == WRITER_UPDATE) {
      new FileSystemFeatureWriterModify(storage, transaction, query.getFilter, config.readThreads)
    } else if ((flags & WRITER_ADD) == WRITER_ADD) {
      new FileSystemFeatureWriterAppend(storage, transaction)
    } else {
      throw new IllegalArgumentException(s"Expected one of $WRITER_ADD or $WRITER_UPDATE, but got: $flags")
    }
  }

  /**
   * Gets the storage transaction associated with the current geotools transaction
   *
   * @return storage transaction
   */
  private def storageTransaction(): StorageTransaction = {
    val transaction = getTransaction
    if (transaction == null || transaction == Transaction.AUTO_COMMIT) { StorageTransaction.AutoCommit } else {
      // note: geotools tracks its own state using the content entry as a key, so we have to use something different
      val key = TransactionKey(getDataStore)
      val state = transaction.synchronized {
        transaction.getState(key) match {
          case null =>
            val state = new FileSystemTransactionState()
            transaction.putState(key, state)
            state

          case state: FileSystemTransactionState => state
          case state => throw new IllegalArgumentException(s"Found non-FileSystem state in transaction: $state")
        }
      }
      state.transaction(storage)
    }
  }

  override def buildFeatureType(): SimpleFeatureType = storage.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = storage.getBounds(query.getFilter, config.readThreads)

  override def getCountInternal(query: Query): Int = {
    val count = storage.getCount(query.getFilter, config.readThreads)
    if (count.isValidInt) { count.toInt } else { Int.MaxValue }
  }

  override def getReaderInternal(original: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    import org.locationtech.geomesa.index.conf.QueryHints._

    val query = new Query(original)
    // The type name can sometimes be empty such as Query.ALL
    query.setTypeName(storage.sft.getTypeName)

    val reader = config.queryTimeout match {
      case None => storage.getReader(query, threads = config.readThreads)
      case Some(timeout) =>
        val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
        new ManagedScan(new FileSystemScanner(storage, query, config.readThreads), Timeout(timeout), query.getTypeName, filter)
    }

    // get a closeable java iterator that DelegateSimpleFeatureIterator will process correctly
    val iter = new FileSystemFeatureIterator(reader)

    // transforms will be set after getting the iterator
    val transformSft = query.getHints.getTransformSchema.getOrElse(storage.sft)

    // note: DelegateSimpleFeatureIterator will close the iterator by checking that it implements Closeable
    new DelegateSimpleFeatureReader(transformSft, new DelegateSimpleFeatureIterator(iter))
  }

  /**
   * Resolves filter property names against the schema, but preserves JSON paths.
   *
   * The base class runs `DataUtilities.resolvePropertyNames`, whose `PropertyNameResolvingVisitor`
   * evaluates each property name against the feature type and rewrites it to the resolved attribute's
   * local name. A JSON path like `$.props.name` evaluates (via GeoMesa's JSON property accessor) to
   * the `props` descriptor, so the default would collapse the whole path to `props` - losing the nested
   * field that IcebergFilterConverter needs to push down. We keep `$`-prefixed names verbatim and
   * resolve everything else as usual.
   *
   * @param query the query being planned
   * @return the query with non-JSON-path property names resolved
   */
  override protected def resolvePropertyNames(query: Query): Query = {
    val filter = query.getFilter
    if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE) { query } else {
      val resolved = filter.accept(new JsonPathPropertyNameResolver(getSchema), null).asInstanceOf[Filter]
      if (resolved == filter) { query } else {
        val newQuery = new Query(query)
        newQuery.setFilter(resolved)
        newQuery
      }
    }
  }

  override def canEvent: Boolean = false
  override def canReproject: Boolean = false
  override def canTransact: Boolean = true
  override def canLimit(query: Query): Boolean = true
  override def canSort(query: Query): Boolean = true
  override def canFilter(query: Query): Boolean = true
  override def canRetype(query: Query): Boolean = true

  override protected def buildQueryCapabilities(): QueryCapabilities = FileSystemFeatureStore.capabilities
}

object FileSystemFeatureStore {

  private val capabilities: QueryCapabilities = new QueryCapabilities() {
    override def isReliableFIDSupported: Boolean = true
    override def isUseProvidedFIDSupported: Boolean = true
  }

  /**
   * Key for tracking transaction state
   *
   * @param ds data store
   */
  private case class TransactionKey(ds: DataStore)

  /**
   * Holder for an open transaction
   *
   * @param storage storage instance
   * @param tx transaction
   */
  private case class TransactionInstance(storage: FileSystemStorage, tx: StorageTransaction)

  /**
   * Ties storage transactions to a geotools transaction. A geotools transaction can be committed or rolled back multiple
   * times, while a storage transaction is single-use - so a new storage transaction is started for each set of changes.
   * Any uncommitted changes are rolled back when the geotools transaction is closed.
   *
   * A single state tracks all the feature types in a data store, as geotools hides the type of any failure if
   * more than one state fails to commit.
   */
  private class FileSystemTransactionState extends Transaction.State {

    // the current storage transactions, if there are any pending changes, keyed by feature type name
    private val current = scala.collection.mutable.LinkedHashMap.empty[String, TransactionInstance]

    /**
     * Gets the current storage transaction, starting a new one if needed
     *
     * @param storage storage instance
     * @return
     */
    def transaction(storage: FileSystemStorage): StorageTransaction = synchronized {
      val typeName = storage.sft.getTypeName
      current.get(typeName) match {
        case None =>
          val tx = storage.newTransaction()
          current.put(typeName, TransactionInstance(storage, tx))
          tx

        case Some(instance) if instance.storage.eq(storage) => instance.tx

        case _ =>
          throw new IllegalStateException(
            s"Schema '$typeName' was modified during the transaction - the transaction must be rolled back")
      }
    }

    override def setTransaction(transaction: Transaction): Unit = {
      if (transaction == null) {
        // the transaction has been closed
        rollback()
      }
    }

    override def addAuthorization(authID: String): Unit = {}

    override def commit(): Unit = synchronized {
      val failures = ArrayBuffer.empty[Throwable]
      current.toList.foreach { case (typeName, instance) =>
        // note: if the commit fails, the storage transaction is generally left open so it can be re-tried or rolled back
        try { instance.tx.commit() } catch {
          case NonFatal(e) => failures += e
        } finally {
          if (!instance.tx.isOpen) {
            current.remove(typeName)
          }
        }
      }
      raise(failures.toSeq)
    }

    override def rollback(): Unit = synchronized {
      val failures = ArrayBuffer.empty[Throwable]
      try {
        current.values.foreach { instance =>
          try { instance.tx.rollback() } catch {
            case NonFatal(e) => failures += e
          }
        }
      } finally {
        current.clear()
      }
      raise(failures.toSeq)
    }

    /**
     * Throws a single exception for any failures. Conflict exceptions take priority, so that they can be
     * handled by the caller - any other failures are added as suppressed exceptions.
     *
     * @param failures failures
     */
    private def raise(failures: Seq[Throwable]): Unit = {
      if (failures.nonEmpty) {
        val primary = failures.find(_.isInstanceOf[ConflictException]).getOrElse(failures.head)
        val e = primary match {
          case e: IOException => e
          case e => new IOException(e.getMessage, e)
        }
        failures.filterNot(_.eq(primary)).foreach(e.addSuppressed)
        throw e
      }
    }
  }

  /**
   * Wrapper for managed scans
   *
   * @param storage storage
   * @param query query
   * @param threads query threads
   */
  private class FileSystemScanner(storage: FileSystemStorage, val query: Query, threads: Int)
      extends LowLevelScanner[SimpleFeature] {

    private var reader: CloseableIterator[SimpleFeature] = _

    override def iterator: Iterator[SimpleFeature] = synchronized {
      reader = storage.getReader(query, threads = threads)
      reader
    }

    override def close(): Unit = synchronized {
      if (reader != null) {
        reader.close()
      }
    }
  }

  /**
    * Iterator for querying file system storage
    *
    * Note: implements Closeable and not AutoCloseable so that DelegateFeatureIterator will close it properly
    *
    * @param iter delegate iterator
    */
  private class FileSystemFeatureIterator(iter: CloseableIterator[SimpleFeature])
      extends java.util.Iterator[SimpleFeature] with Closeable {
    override def hasNext: Boolean = iter.hasNext
    override def next(): SimpleFeature = iter.next()
    override def close(): Unit = iter.close()
  }

  /**
   * Appending feature writer
   *
   * @param storage storage instance
   * @param transaction transaction
   */
  private class FileSystemFeatureWriterAppend(storage: FileSystemStorage, transaction: StorageTransaction)
      extends FastSettableFeatureWriter {

    private val writer = storage.getMultiPartitionWriter(transaction)
    private val featureIds = new AtomicLong(0)
    private var feature: ScalaSimpleFeature = _

    override def getFeatureType: SimpleFeatureType = storage.sft

    override def hasNext: Boolean = false

    override def next(): ScalaSimpleFeature = {
      feature = new ScalaSimpleFeature(storage.sft, featureIds.getAndIncrement().toString)
      feature
    }

    override def write(): Unit = writer.write(GeoMesaFeatureWriter.featureWithFid(feature))

    override def remove(): Unit = throw new IllegalArgumentException("This writer is append only")

    override def close(): Unit = writer.close()
  }

  /**
    * Modifying feature writer
    *
    * @param storage instance
    * @param transaction transaction
    * @param filter query filter
    * @param readThreads read threads
    */
  private class FileSystemFeatureWriterModify(
      storage: FileSystemStorage,
      transaction: StorageTransaction,
      filter: Filter,
      readThreads: Int
    ) extends FeatureWriter[SimpleFeatureType, SimpleFeature] with LazyLogging {

    private val writer = storage.getUpdateWriter(filter, readThreads, transaction)

    override def getFeatureType: SimpleFeatureType = storage.sft

    override def hasNext: Boolean = writer.hasNext
    // TODO implement FastSettableFeatureWriter and wire ScalaSimpleFeature all the way through
    override def next(): SimpleFeature = writer.next()
    override def write(): Unit = writer.write()
    override def remove(): Unit = writer.remove()
    override def close(): Unit = writer.close()
  }
}
