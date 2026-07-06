/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.deletes.PositionDelete
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.{DeleteFile, Schema}
import org.apache.parquet.schema.MessageType
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.{FastSettableFeature, ScalaSimpleFeature}
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemUpdateWriter
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergUpdateWriter.MultiPartitionDeleter
import org.locationtech.geomesa.fs.storage.core.utils.{MultiPartitionAction, MultiPartitionWriter}
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, Partition}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FlushQuietly}

import java.io.{Closeable, Flushable}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Update writer implementation
 *
 * @param storage file system storage instance
 * @param reader features to update
 * @param writer writer for updated features
 * @param deleter deleter for removed/modified features
 */
class IcebergUpdateWriter(
    storage: FileSystemStorage,
    reader: CloseableIterator[SimpleFeature],
    writer: MultiPartitionWriter,
    deleter: MultiPartitionDeleter,
  ) extends FileSystemUpdateWriter {

  // feature returned from reader
  private var original: SimpleFeature = _

  // feature that caller will modify
  private var live: FastSettableFeature = _

  override def hasNext: Boolean = reader.hasNext

  override def next(): FastSettableFeature = {
    original = reader.next()
    live = ScalaSimpleFeature.copy(storage.sft, original) // this copies user data as well
    // set the use provided FID hint - allows user to update fid if desired,
    // but if not we'll use the existing one
    live.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    live
  }

  override def write(): Unit = {
    if (original == null) {
      throw new IllegalStateException("next() must be called before write()")
    }
    // update the feature id based on hints before we compare for changes
    live = GeoMesaFeatureWriter.featureWithFid(live)
    // only write if feature has actually changed...
    // comparison of feature ID and attributes - doesn't consider concrete class used
    if (!ScalaSimpleFeature.equalIdAndAttributes(live, original) ||
        SecurityUtils.getVisibility(live) != SecurityUtils.getVisibility(original)) {
      deleter(original)
      writer(live)
    }
    original = null
    live = null
  }

  override def remove(): Unit = {
    if (original == null) {
      throw new IllegalStateException("next() must be called before remove()")
    }
    deleter(original)
    original = null
    live = null
  }

  override def flush(): Unit = FlushQuietly.raise(writer, deleter)

  override def close(): Unit = {
    CloseWithLogging.raise(Seq(reader, writer, deleter))
    if (!deleter.files.isEmpty) {
      val delta = storage.table.newRowDelta()
      deleter.files.forEach(f => delta.addDeletes(f))
      delta.commit()
    }
  }
}

object IcebergUpdateWriter {

  /**
   * Create a new update writer
   *
   * @param storage storage
   * @param filter filter for features to update
   * @param readThreads number of threads using to read features to update
   * @param maxOpenPartitions max open writer partitions
   * @return
   */
  def apply(storage: FileSystemStorage, filter: Filter, readThreads: Int, maxOpenPartitions: Int): IcebergUpdateWriter = {
    // note: writer and deleter are Closeable, but they don't need to be closed unless they initialize at least one partition,
    // so it's safe to not clean them up if there's an exception in this method
    val writer = new MultiPartitionWriter(storage, maxOpenPartitions)
    val deleter = new MultiPartitionDeleter(storage, maxOpenPartitions)
    // note: this class expects the reader to return RecordSimpleFeatures, but this isn't enforced through the call signature
    val reader = storage.getReader(new Query(storage.sft.getTypeName, filter), readThreads, forUpdate = true)
    new IcebergUpdateWriter(storage, reader, writer, deleter)
  }

  /**
   * Multi-partition deleter
   *
   * @param storage file system storage instance
   * @param maxOpenPartitions max open partition writers
   */
  private class MultiPartitionDeleter(storage: FileSystemStorage, maxOpenPartitions: Int)
      extends MultiPartitionAction[DeleteWriter](storage, maxOpenPartitions) {

    val files: java.util.Set[DeleteFile] = Collections.newSetFromMap(new ConcurrentHashMap[DeleteFile, java.lang.Boolean]())

    override protected def createAction(partition: Partition): DeleteWriter = new DeleteWriter(storage, partition, files)
    override protected def apply(action: DeleteWriter, feature: SimpleFeature): Unit =
      action.apply(feature.asInstanceOf[RecordSimpleFeature])
  }

  /**
   * Single partition deleter
   *
   * @param storage file system storage instance
   * @param partition partition to delete records out of
   * @param files set for returning any delete files created by this deleter
   */
  private class DeleteWriter(storage: FileSystemStorage, partition: Partition, files: java.util.Set[DeleteFile])
      extends (RecordSimpleFeature => Unit) with Closeable with Flushable {

    private val closed = new AtomicBoolean(false)
    private val deletes = Seq.newBuilder[(String, Long)]
    private val path = storage.context.root.resolve(FileSystemStorage.newFilePath(storage.sft.getTypeName, "x-"))
    private val file = storage.table.io().newOutputFile(path.toString)

    private val writer =
      Parquet.writeDeletes(file)
        .withSpec(storage.table.spec())
        .withPartition(storage.metadata.partition(partition))
        .createWriterFunc((schema: Schema, `type`: MessageType) => GenericParquetWriter.create(schema, `type`))
        .buildPositionWriter[Unit]()

    override def apply(feature: RecordSimpleFeature): Unit = deletes += (feature.getFilePath -> feature.getRowPosition)

    override def flush(): Unit = {}

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        try {
          val struct = PositionDelete.create[Unit]()
          // note: deletes need to be sorted by file+row
          deletes.result().sorted.foreach { case (path, pos) =>
            writer.write(struct.set(path, pos))
          }
        } finally{
          CloseWithLogging(writer)
        }
        files.add(writer.toDeleteFile)
      }
    }
  }
}
