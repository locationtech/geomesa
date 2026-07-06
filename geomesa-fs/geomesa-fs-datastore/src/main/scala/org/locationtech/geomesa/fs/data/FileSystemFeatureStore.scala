/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.types.{Conversions, Types}
import org.geotools.api.data.{FeatureReader, FeatureWriter, Query, QueryCapabilities}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore.FileSystemDataStoreConfig
import org.locationtech.geomesa.fs.data.FileSystemFeatureStore._
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName}
import org.locationtech.geomesa.index.geotools.{FastSettableFeatureWriter, GeoMesaFeatureWriter}
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.io.Closeable
import java.util.concurrent.atomic.AtomicLong

class FileSystemFeatureStore(
    val storage: FileSystemStorage,
    entry: ContentEntry,
    query: Query,
    config: FileSystemDataStoreConfig,
  ) extends ContentFeatureStore(entry, query) with LazyLogging {

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    // note: check update first as sometimes we get ADD | UPDATE
    if ((flags & WRITER_UPDATE) == WRITER_UPDATE) {
      new FileSystemFeatureWriterModify(storage, query.getFilter, config.readThreads)
    } else if ((flags & WRITER_ADD) == WRITER_ADD) {
      new FileSystemFeatureWriterAppend(storage)
    } else {
      throw new IllegalArgumentException(s"Expected one of $WRITER_ADD or $WRITER_UPDATE, but got: $flags")
    }
  }

  override def buildFeatureType(): SimpleFeatureType = storage.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = {
    val envelope = new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
    val bboxFieldName = BoundingBoxField.groupName(ColumnName.encode(storage.sft.getGeometryDescriptor.getLocalName))
    val bboxField = storage.schema.schema.findField(bboxFieldName).`type`().asStructType()
    val (minFieldIds, maxFieldIds) =
      Seq(BoundingBoxField.XMin, BoundingBoxField.YMin, BoundingBoxField.XMax, BoundingBoxField.YMax)
        .map(f => bboxField.field(f).fieldId())
        .splitAt(2)
    storage.metadata.files().includeFileStats().forFilter(query.getFilter).scan().foreach { f =>
      val minBuffers = minFieldIds.map(f.lowerBounds().get)
      val maxBuffers = maxFieldIds.map(f.upperBounds().get)
      if (!minBuffers.contains(null) && !maxBuffers.contains(null)) {
        val Seq(xmin, ymin) = minBuffers.map(Conversions.fromByteBuffer[Float](Types.FloatType.get(), _))
        val Seq(xmax, ymax) = maxBuffers.map(Conversions.fromByteBuffer[Float](Types.FloatType.get(), _))
        envelope.expandToInclude(xmin, ymin)
        envelope.expandToInclude(xmax, ymax)
      }
    }
    envelope
  }

  override def getCountInternal(query: Query): Int = {
    val count = storage.metadata.files().forFilter(query.getFilter).scan().map(_.recordCount()).sum
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
   */
  private class FileSystemFeatureWriterAppend(storage: FileSystemStorage) extends FastSettableFeatureWriter {

    private val writer = storage.getMultiPartitionWriter()
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
    * @param filter query filter
    * @param readThreads read threads
    */
  private class FileSystemFeatureWriterModify(storage: FileSystemStorage, filter: Filter, readThreads: Int)
      extends FeatureWriter[SimpleFeatureType, SimpleFeature] with LazyLogging {

    private val writer = storage.getWriter(filter, threads = readThreads)

    override def getFeatureType: SimpleFeatureType = storage.sft

    override def hasNext: Boolean = writer.hasNext
    // TODO implement FastSettableFeatureWriter and wire ScalaSimpleFeature all the way through
    override def next(): SimpleFeature = writer.next()
    override def write(): Unit = writer.write()
    override def remove(): Unit = writer.remove()
    override def close(): Unit = writer.close()
  }
}
