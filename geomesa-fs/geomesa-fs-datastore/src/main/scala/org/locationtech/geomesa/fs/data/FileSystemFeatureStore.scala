/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{FeatureReader, FeatureWriter, Query, QueryCapabilities}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore.FileSystemDataStoreConfig
import org.locationtech.geomesa.fs.data.FileSystemFeatureStore._
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.core.{CloseableFeatureIterator, FileSystemStorage, Partition}
import org.locationtech.geomesa.index.geotools.{FastSettableFeatureWriter, GeoMesaFeatureWriter}
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging}
import org.locationtech.jts.geom.Geometry

import java.io.Closeable
import java.util.Map.Entry
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, Executors, RejectedExecutionException, TimeUnit}
import java.util.function.BiFunction

class FileSystemFeatureStore(
    val storage: FileSystemStorage,
    entry: ContentEntry,
    query: Query,
    config: FileSystemDataStoreConfig,
  ) extends ContentFeatureStore(entry, query) with LazyLogging {

  private val sft = storage.metadata.sft

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    // note: check update first as sometimes we get ADD | UPDATE
    if ((flags & WRITER_UPDATE) == WRITER_UPDATE) {
      new FileSystemFeatureWriterModify(storage, sft, query.getFilter, config.readThreads)
    } else if ((flags & WRITER_ADD) == WRITER_ADD) {
      new FileSystemFeatureWriterAppend(storage, sft, config.writeMaxOpenPartitions, config.writeTimeout.toMillis)
    } else {
      throw new IllegalArgumentException(s"Expected one of $WRITER_ADD or $WRITER_UPDATE, but got: $flags")
    }
  }

  override def buildFeatureType(): SimpleFeatureType = sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = {
    val envelope = new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
    Option(sft.getGeometryDescriptor).foreach { g =>
      val i = sft.indexOf(g.getLocalName)
      storage.metadata.getFiles(query.getFilter).foreach { file =>
        file.bounds.find(_.attribute == i).foreach { b =>
          b.decode(sft).productIterator.foreach {
            case g: Geometry => envelope.expandToInclude(g.getEnvelopeInternal)
          }
        }
      }
    }
    envelope
  }

  override def getCountInternal(query: Query): Int =
    storage.metadata.getFiles(query.getFilter).map(_.count).sum.toInt

  override def getReaderInternal(original: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    import org.locationtech.geomesa.index.conf.QueryHints._

    val query = new Query(original)
    // The type name can sometimes be empty such as Query.ALL
    query.setTypeName(sft.getTypeName)

    val reader = config.queryTimeout match {
      case None => storage.getReader(query, threads = config.readThreads)
      case Some(timeout) =>
        val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
        new ManagedScan(new FileSystemScanner(storage, query, config.readThreads), Timeout(timeout), query.getTypeName, filter)
    }

    // get a closeable java iterator that DelegateSimpleFeatureIterator will process correctly
    val iter = new FileSystemFeatureIterator(reader)

    // transforms will be set after getting the iterator
    val transformSft = query.getHints.getTransformSchema.getOrElse(sft)

    // note: DelegateSimpleFeatureIterator will close the iterator by checking that it implements Closeable
    new DelegateSimpleFeatureReader(transformSft, new DelegateSimpleFeatureIterator(iter))
  }

  override def canTransact: Boolean = false
  override def canEvent: Boolean = false
  override def canReproject: Boolean = false
  override def canLimit(query: Query): Boolean = true
  override def canSort(query: Query): Boolean = true
  override def canFilter(query: Query): Boolean = true
  override def canRetype(query: Query): Boolean = true

  override protected def buildQueryCapabilities(): QueryCapabilities = FileSystemFeatureStore.capabilities
}

object FileSystemFeatureStore {

  import scala.collection.JavaConverters._

  private val capabilities: QueryCapabilities = new QueryCapabilities() {
    override def isReliableFIDSupported: Boolean = true
    override def isUseProvidedFIDSupported: Boolean = true
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

    private var reader: CloseableFeatureIterator = _

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
  private class FileSystemFeatureIterator(iter: CloseableFeatureIterator)
      extends java.util.Iterator[SimpleFeature] with Closeable {
    override def hasNext: Boolean = iter.hasNext
    override def next(): SimpleFeature = iter.next()
    override def close(): Unit = iter.close()
  }

  /**
   * Appending feature writer
   *
   * @param storage storage instance
   * @param sft simple feature type
   * @param maxOpenPartitions max open partition writers
   * @param writerTimeoutMillis write timeout, for flushing partitions
   */
  private class FileSystemFeatureWriterAppend(
      storage: FileSystemStorage,
      sft: SimpleFeatureType,
      maxOpenPartitions: Int,
      writerTimeoutMillis: Long
    ) extends FastSettableFeatureWriter with LazyLogging {

    private val writers = new ConcurrentHashMap[Partition, WriterHolder]()

    private val computeForWrite: BiFunction[Partition, WriterHolder, WriterHolder] = (p, current) => {
      if (current == null) { new WriterHolder(storage.getWriter(p)) } else {
        current.updateAccessTime() // update access time before returning so it's not expired
        current
      }
    }

    private val computeForExpire: BiFunction[Partition, WriterHolder, WriterHolder] = (p, current) => {
      // double check that the writer is still expired, as this method is atomic wrt computeForWrite
      if (current != null && current.age >= writerTimeoutMillis) {
        logger.debug(
          s"Closing writer for partition $p (last accessed ${current.age}ms ago) due to hitting " +
            s"expiry ${writerTimeoutMillis}ms")
        CloseWithLogging(current)
        null
      } else {
        current
      }
    }

    private val ex = Executors.newSingleThreadScheduledExecutor()

    private val expire: Runnable = () => {
      var nextRun = writerTimeoutMillis
      writers.entrySet().forEach { entry =>
        val delta = writerTimeoutMillis - entry.getValue.age
        if (delta < 1) {
          writers.computeIfPresent(entry.getKey, computeForExpire)
        } else {
          nextRun = math.min(delta, nextRun)
        }
      }
      if (!ex.isShutdown) {
        try {
          ex.schedule(expire, nextRun + 1000, TimeUnit.MILLISECONDS)
        } catch {
          case _: RejectedExecutionException => // executor was shutdown due to close
        }
      }
    }
    // schedule the first expiry check
    ex.schedule(expire, writerTimeoutMillis + 1000, TimeUnit.MILLISECONDS)

    private val featureIds = new AtomicLong(0)
    private var feature: ScalaSimpleFeature = _

    override def getFeatureType: SimpleFeatureType = sft

    override def hasNext: Boolean = false

    override def next(): ScalaSimpleFeature = {
      feature = new ScalaSimpleFeature(sft, featureIds.getAndIncrement().toString)
      feature
    }

    override def write(): Unit = {
      val sf = GeoMesaFeatureWriter.featureWithFid(feature)
      val partition = Partition(storage.metadata.schemes.map(_.getPartition(sf)))
      writers.compute(partition, computeForWrite).write(sf)
      feature = null
      if (writers.size() > maxOpenPartitions) {
        val key = writers.reduceEntries(maxOpenPartitions / 2, WriterHolder.Oldest).getKey
        val expired = writers.remove(key)
        // may be null if it was expired asynchronously due to inactivity
        if (expired != null) {
          logger.debug(
            s"Closing writer for partition $key (last accessed ${expired.age}ms ago) due to hitting max writer " +
              s"threshold $maxOpenPartitions")
          CloseWithLogging(expired)
        }
      }
    }

    override def remove(): Unit = throw new IllegalArgumentException("This writer is append only")

    override def close(): Unit = {
      try {
        ex.shutdownNow()
        // ensure all writers are closed before returning from this method
        ex.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
      } finally {
        CloseQuietly(writers.values().asScala.toSeq).foreach(e => throw e)
      }
    }
  }

  private class WriterHolder(writer: FileSystemWriter) extends Closeable {

    @volatile
    private var lastAccess: Long = System.currentTimeMillis()
    private val closed: AtomicBoolean = new AtomicBoolean(false)

    def write(sf: SimpleFeature): Unit = writer.write(sf)
    def age: Long = System.currentTimeMillis() - lastAccess
    def updateAccessTime(): Unit = lastAccess = System.currentTimeMillis()

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        writer.close()
      }
    }
  }

  private object WriterHolder {
    val Oldest: BiFunction[Entry[Partition, WriterHolder], Entry[Partition, WriterHolder], Entry[Partition, WriterHolder]] =
      (left, right) => if (right.getValue.age < left.getValue.age) { right } else { left }
  }

  /**
    * Modifying feature writer
    *
    * @param storage instance
    * @param sft simple feature type
    * @param filter query filter
    * @param readThreads read threads
    */
  private class FileSystemFeatureWriterModify(
      storage: FileSystemStorage,
      sft: SimpleFeatureType,
      filter: Filter,
      readThreads: Int
    ) extends FeatureWriter[SimpleFeatureType, SimpleFeature] with LazyLogging {

    private val writer = storage.getWriter(filter, threads = readThreads)

    override def getFeatureType: SimpleFeatureType = sft

    override def hasNext: Boolean = writer.hasNext
    // TODO implement FastSettableFeatureWriter and wire ScalaSimpleFeature all the way through
    override def next(): SimpleFeature = writer.next()
    override def write(): Unit = writer.write()
    override def remove(): Unit = writer.remove()
    override def close(): Unit = writer.close()
  }
}
