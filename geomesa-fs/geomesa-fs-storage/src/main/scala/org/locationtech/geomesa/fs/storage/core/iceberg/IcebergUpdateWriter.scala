/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.{FastSettableFeature, ScalaSimpleFeature}
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemUpdateWriter
import org.locationtech.geomesa.fs.storage.core.StorageTransaction.TableTransaction
import org.locationtech.geomesa.fs.storage.core.utils.MultiPartitionWriter
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/**
 * Update writer implementation. Updated features are written to new data files, and the original rows are deleted
 * using deletion vectors. All changes are tracked in a transaction, and are not visible until it is committed.
 *
 * @param storage file system storage instance
 * @param transaction transaction tracking the changes
 * @param reader features to update
 * @param writer writer for updated features
 */
class IcebergUpdateWriter(
    storage: FileSystemStorage,
    transaction: TableTransaction,
    reader: CloseableIterator[SimpleFeature],
    writer: MultiPartitionWriter,
  ) extends FileSystemUpdateWriter {

  private val closed = new AtomicBoolean(false)

  // feature returned from reader
  private var original: SimpleFeature = _

  // feature that caller will modify
  private var live: FastSettableFeature = _

  transaction.opened()

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
      val row = original.asInstanceOf[StructSimpleFeature]
      val path = row.getFilePath
      val position = row.getRowPosition
      // check that the original can be deleted before writing the update, so that we don't end up with both.
      // write before deleting to ensure the feature is valid
      transaction.validateDelete(path, position)
      writer.write(live)
      transaction.deleteValidated(path, position)
    }
    original = null
    live = null
  }

  override def remove(): Unit = {
    if (original == null) {
      throw new IllegalStateException("next() must be called before remove()")
    }
    val row = original.asInstanceOf[StructSimpleFeature]
    transaction.delete(row.getFilePath, row.getRowPosition)
    original = null
    live = null
  }

  override def flush(): Unit = writer.flush()

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try { CloseWithLogging.raise(Seq(reader, writer)) } catch {
        case NonFatal(e) => transaction.failed(e); throw e
      } finally {
        transaction.closed()
      }
    }
  }
}

object IcebergUpdateWriter {

  /**
   * Create a new update writer
   *
   * @param storage storage
   * @param transaction transaction
   * @param filter filter for features to update
   * @param readThreads number of threads using to read features to update
   * @param maxOpenPartitions max open writer partitions
   * @return
   */
  def apply(
      storage: FileSystemStorage,
      transaction: TableTransaction,
      filter: Filter,
      readThreads: Int,
      maxOpenPartitions: Int): IcebergUpdateWriter = {
    val writer = new MultiPartitionWriter(storage, transaction, maxOpenPartitions)
    try {
      // read from the same snapshot that deletes are validated against - if the table is empty, there's nothing to update
      // note: this class expects the reader to return StructSimpleFeatures, but this isn't enforced through the call signature
      val reader = transaction.snapshotId match {
        case None => CloseableIterator.empty[SimpleFeature]
        case id => storage.getReader(new Query(storage.sft.getTypeName, filter), readThreads, forUpdate = true, snapshotId = id)
      }
      try { new IcebergUpdateWriter(storage, transaction, reader, writer) } catch {
        case NonFatal(e) => CloseWithLogging(reader); throw e
      }
    } catch {
      case NonFatal(e) => CloseWithLogging(writer); throw e
    }
  }
}
