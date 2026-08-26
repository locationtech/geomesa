/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.data.{BaseDeleteLoader, DeleteLoader}
import org.apache.iceberg.deletes.{BaseDVFileWriter, DVFileWriter, PositionDeleteIndex}
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.util.{CharSequenceMap, DeleteFileSet}
import org.apache.iceberg.{DeleteFile, ManifestFiles, PartitionSpec, StructLike}
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
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FlushQuietly, WithClose}

import java.io.{Closeable, Flushable}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.{Function, Supplier}

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
      // guard against concurrent commits that touch the same delete files
      Option(storage.table.currentSnapshot()).foreach(s => delta.validateFromSnapshot(s.snapshotId()))
      deleter.files.forEach(f => delta.addDeletes(f))
      // remove any previous DVs whose deletes we merged into the newly written DVs, otherwise
      // the commit will fail validation with multiple live DVs referencing the same data file
      deleter.rewritten.forEach(f => delta.removeDeletes(f))
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

    // newly written delete files (DVs) to add on commit
    val files: java.util.Set[DeleteFile] = Collections.newSetFromMap(new ConcurrentHashMap[DeleteFile, java.lang.Boolean]())
    // previous DVs whose deletes we merged into the new DVs - these must be removed on commit
    val rewritten: java.util.Set[DeleteFile] = Collections.newSetFromMap(new ConcurrentHashMap[DeleteFile, java.lang.Boolean]())

    private val closed = new AtomicBoolean(false)
    private val fileSupplier: Supplier[OutputFile] = () => storage.table.io().newOutputFile(storage.newFilePath("puffin"))
    // loads existing deletes for a data file so they can be merged into the new DV - a data file
    // can only be referenced by a single DV, so we have to rewrite any existing one
    private val writer = new BaseDVFileWriter(fileSupplier, new PreviousDeleteLoader(storage))

    override protected def createAction(partition: Partition): DeleteWriter =
      new DeleteWriter(writer, storage.table.spec(), storage.metadata.partition(partition))
    override protected def apply(action: DeleteWriter, feature: SimpleFeature): Unit =
      action.apply(feature.asInstanceOf[StructSimpleFeature])

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        try { super.close() } finally  {
          CloseWithLogging(writer)
        }
        val result = writer.result()
        files.addAll(result.deleteFiles())
        rewritten.addAll(result.rewrittenDeleteFiles())
      }
    }
  }

  /**
   * Loads existing deletes (deletion vectors) for a given data file path, so they can be merged into a
   * newly written DV. Returns null if there are no existing deletes for the given path.
   *
   * @param storage file system storage instance
   */
  private class PreviousDeleteLoader(storage: FileSystemStorage) extends Function[String, PositionDeleteIndex] {

    import scala.collection.JavaConverters._

    // map of data-file-path -> set of delete files that reference it, from the current table state
    private lazy val deletesByPath: CharSequenceMap[DeleteFileSet] = {
      val map = CharSequenceMap.create[DeleteFileSet]()
      val snapshot = storage.table.currentSnapshot()
      if (snapshot != null) {
        val io = storage.table.io()
        val specs = storage.table.specs()
        snapshot.deleteManifests(io).asScala.foreach { manifest =>
          WithClose(ManifestFiles.readDeleteManifest(manifest, io, specs)) { reader =>
            reader.iterator().asScala.foreach { deleteFile =>
              val ref = deleteFile.referencedDataFile()
              if (ref != null) {
                // copy() to detach from the manifest reader, which reuses containers
                map.computeIfAbsent(ref, () => DeleteFileSet.create()).add(deleteFile.copy())
              }
            }
          }
        }
      }
      map
    }

    private lazy val loader: DeleteLoader =
      new BaseDeleteLoader(deleteFile => storage.table.io().newInputFile(deleteFile))

    override def apply(path: String): PositionDeleteIndex = {
      val set = deletesByPath.get(path)
      if (set == null) { null } else {
        loader.loadPositionDeletes(set, path)
      }
    }
  }

  /**
   * Single partition deleter
   *
   * @param writer delete writer
   * @param spec table partition spec
   * @param partition partition to delete records out of
   */
  private class DeleteWriter(writer: DVFileWriter, spec: PartitionSpec, partition: StructLike)
      extends (StructSimpleFeature => Unit) with Closeable with Flushable {

    override def apply(feature: StructSimpleFeature): Unit =
      writer.delete(feature.getFilePath, feature.getRowPosition, spec, partition)

    override def flush(): Unit = {}
    override def close(): Unit = {}
  }
}
