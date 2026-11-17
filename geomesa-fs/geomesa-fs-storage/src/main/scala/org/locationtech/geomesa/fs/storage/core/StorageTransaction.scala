/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg._
import org.apache.iceberg.data.{BaseDeleteLoader, DeleteLoader}
import org.apache.iceberg.deletes.{BaseDVFileWriter, Deletes, PositionDeleteIndex}
import org.apache.iceberg.exceptions.{CommitStateUnknownException, ValidationException}
import org.apache.iceberg.io.{CloseableIterable, DeleteWriteResult, FileIO, InputFile, OutputFile, OutputFileFactory}
import org.apache.iceberg.util.{CharSequenceMap, DeleteFileSet, PropertyUtil}
import org.locationtech.geomesa.fs.storage.core.StorageTransaction.ConflictException
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.{Closeable, IOException}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * A transaction against a file system storage instance. Writers created with a transaction do not modify the
 * table - instead, all the data files and row-level deletes they produce are published in a single atomic commit
 * when the transaction is committed, or discarded when it is rolled back.
 *
 * Reads never see uncommitted changes, including reads made by update writers in the same transaction. As a
 * consequence, a row can only be modified once per transaction.
 *
 * A transaction is single-use - once it has been committed or rolled back, it can't be used again. Instances
 * are thread-safe.
 *
 * Transactions are created through `FileSystemStorage.newTransaction`. Alternatively, `StorageTransaction.AutoCommit`
 * may be used to have each writer commit its own changes when it is closed.
 */
sealed trait StorageTransaction {

  /**
   * Is this transaction still usable, i.e. it has not been committed or rolled back
   *
   * @return
   */
  def isOpen: Boolean

  /**
   * Publishes all changes made in this transaction in a single commit. All writers must be closed first.
   *
   * If the commit fails, the transaction is left as-is, and may be re-tried or rolled back - unless the result
   * of the commit is unknown, in which case the transaction is finished, and any files are left in place.
   *
   * @throws ConflictException if a concurrent operation modified the same rows or removed the files they are in
   */
  @throws(classOf[ConflictException])
  def commit(): Unit

  /**
   * Discards all changes made in this transaction, deleting any files that were written. Any writers that are
   * still open will fail when they are closed.
   *
   * Has no effect if the transaction has already been committed or rolled back.
   */
  def rollback(): Unit
}

object StorageTransaction {

  /**
   * Used in place of a transaction to indicate that each writer should commit its own changes when it is closed
   */
  case object AutoCommit extends StorageTransaction {
    override def isOpen: Boolean = true
    override def commit(): Unit = {}
    override def rollback(): Unit = throw new UnsupportedOperationException("Auto-commit can't be rolled back")
  }

  /**
   * Transaction implementation, which tracks the files and deletes created by writers
   *
   * @param storage storage instance
   */
  class TableTransaction private[core] (storage: FileSystemStorage) extends StorageTransaction with LazyLogging {

    import scala.collection.JavaConverters._

    private[core] val table: Table = storage.table

    // new data files to add on commit
    private val dataFiles = ArrayBuffer.empty[DataFile]
    // deleted row positions, keyed by data file path
    private val deletes = scala.collection.mutable.LinkedHashMap.empty[String, PendingDeletes]
    // callbacks to invoke after a successful commit, by key
    private val callbacks = scala.collection.mutable.LinkedHashMap.empty[String, Closeable]

    // table state that deletes are evaluated against - loaded when first needed, so that append-only transactions
    // don't have to refresh the table
    private var view: View = _
    // number of open writers
    private var open = 0
    // first failure from a writer, which prevents the transaction from being committed
    private var failure: Throwable = _
    // set once the transaction is finished, to a description of how it finished
    private var finished: String = _

    override def isOpen: Boolean = synchronized(finished == null)

    override def commit(): Unit = {
      val toClose = synchronized {
        doCommit()
        val toClose = callbacks.values.toList
        finish("committed")
        toClose
      }
      // note: run outside of the lock, as the callbacks may be slow
      toClose.foreach(CloseWithLogging(_))
    }

    private def doCommit(): Unit = {
      checkOpen()
      if (open > 0) {
        throw new IllegalStateException(s"Cannot commit a transaction with $open open writer(s) - close all writers first")
      } else if (failure != null) {
        throw new IllegalStateException("Cannot commit a transaction after a failed write - it must be rolled back", failure)
      }
      if (dataFiles.nonEmpty || deletes.nonEmpty) {
        val retries =
          PropertyUtil.propertyAsInt(table.properties(), TableProperties.COMMIT_NUM_RETRIES, TableProperties.COMMIT_NUM_RETRIES_DEFAULT)
        var attempt = 0
        var done = false
        while (!done) {
          val puffins = ArrayBuffer.empty[String]
          try {
            val delta = table.newRowDelta()
            dataFiles.foreach(delta.addRows)
            if (deletes.nonEmpty) {
              val result = writeDeletes(puffins)
              result.deleteFiles().forEach(f => delta.addDeletes(f))
              // remove any previous DVs whose deletes we merged into the newly written DVs - a data file can only
              // be referenced by a single DV
              result.rewrittenDeleteFiles().forEach(f => delta.removeDeletes(f))
              // fail if any of the files we're deleting from, or the DVs we're replacing, have been removed.
              // iceberg always fails if a DV has been added for the same data file, which is handled below
              pinned().snapshot.foreach(s => delta.validateFromSnapshot(s.snapshotId()))
              delta.validateDataFilesExist(result.referencedDataFiles()).validateDeletedFiles()
            }
            delta.commit()
            done = true
          } catch {
            case e: CommitStateUnknownException =>
              // we can't tell if the files are referenced by the table or not, so leave them for orphan cleanup
              logger.warn(s"Leaving ${dataFiles.size} data file(s) in place as the result of the commit is unknown")
              finish("committed with an unknown result")
              throw e

            case NonFatal(e) =>
              // the deletes are re-written on each attempt, as they have to be merged with the current table state
              puffins.foreach(deleteQuietly)
              if (deletes.isEmpty || !isConflict(e)) {
                throw e
              } else if (attempt >= retries) {
                throw new ConflictException(s"Transaction could not be committed after ${attempt + 1} attempt(s) due to concurrent modifications", e)
              }
              attempt += 1
              rebase(e)
          }
        }
      }
    }

    override def rollback(): Unit = synchronized {
      if (finished == null) {
        dataFiles.foreach(f => deleteQuietly(f.location()))
        finish("rolled back")
      }
    }

    /**
     * Commits the transaction, rolling it back if the commit fails (unless the result of the commit is unknown)
     */
    private[core] def commitOrRollback(): Unit = {
      try { commit() } catch {
        case e: CommitStateUnknownException => throw e
        case NonFatal(e) => rollbackQuietly(e); throw e
      }
    }

    /**
     * Closes a writer and then commits the transaction, rolling it back if either step fails
     *
     * @param close closes the writer
     */
    private[core] def autoCommit(close: => Unit): Unit = {
      try { close } catch {
        case NonFatal(e) => rollbackQuietly(e); throw e
      }
      commitOrRollback()
    }

    /**
     * Rolls back the transaction after a failure, without masking the original failure
     *
     * @param cause failure that caused the rollback
     */
    private[core] def rollbackQuietly(cause: Throwable): Unit = {
      try { rollback() } catch {
        case NonFatal(e) => cause.addSuppressed(e)
      }
    }

    /**
     * Gets the snapshot that updates in this transaction read from, if any
     *
     * @return
     */
    private[core] def snapshotId: Option[Long] = synchronized {
      checkOpen()
      pinned().snapshot.map(_.snapshotId())
    }

    /**
     * Registers a writer with this transaction
     */
    private[core] def opened(): Unit = synchronized {
      checkOpen()
      open += 1
    }

    /**
     * Indicates that a writer has been closed
     */
    private[core] def closed(): Unit = synchronized(open -= 1)

    /**
     * Records a failure, which will prevent this transaction from being committed
     *
     * @param e failure
     */
    private[core] def failed(e: Throwable): Unit = synchronized {
      if (failure == null) {
        failure = e
      }
    }

    /**
     * Adds a new data file
     *
     * @param file data file
     */
    private[core] def add(file: DataFile): Unit = synchronized {
      checkOpen()
      dataFiles += file
    }

    /**
     * Registers a callback that will be closed after a successful commit, and discarded on rollback. Only the
     * most recent callback for a given key is kept. If the transaction is already finished, the callback is discarded.
     *
     * @param key callback key
     * @param callback callback
     */
    private[core] def afterCommit(key: String, callback: Closeable): Unit = synchronized {
      if (finished == null) {
        callbacks.put(key, callback)
      }
    }

    /**
     * Checks that a row can be deleted, without deleting it
     *
     * @param path data file path
     * @param position row position in the file
     * @throws ConflictException if the row can't be deleted
     */
    private[core] def validateDelete(path: String, position: Long): Unit = synchronized {
      checkOpen()
      validate(pinned(), path, position, null)
      if (deletes.get(path).exists(_.positions.isDeleted(position))) {
        throw new ConflictException(s"Row has already been modified in this transaction: $path:$position", null)
      }
    }

    /**
     * Deletes a row
     *
     * @param path data file path
     * @param position row position in the file
     * @throws ConflictException if the row can't be deleted
     */
    private[core] def delete(path: String, position: Long): Unit = synchronized {
      validateDelete(path, position)
      deleteValidated(path, position)
    }

    /**
     * Deletes a row that has already been checked with `validateDelete`, without checking it again
     *
     * @param path data file path
     * @param position row position in the file
     */
    private[core] def deleteValidated(path: String, position: Long): Unit = synchronized {
      checkOpen()
      val pending = deletes.getOrElseUpdate(path, {
        val (specId, partition) = pinned().dataFile(path)
        PendingDeletes(table.specs().get(specId), partition, Deletes.toPositionIndex(CloseableIterable.empty[java.lang.Long]()))
      })
      pending.positions.delete(position)
    }

    /**
     * Validates that a row is live in the given table state
     */
    private def validate(view: View, path: String, position: Long, cause: Throwable): Unit = {
      if (view.dataFile(path) == null) {
        throw new ConflictException(s"Data file has been removed by another operation: $path", cause)
      }
      val previous = view.previousDeletes(path)
      if (previous != null && previous.isDeleted(position)) {
        throw new ConflictException(s"Row has been modified by another operation: $path:$position", cause)
      }
    }

    /**
     * Re-evaluates the pending deletes against the current table state, after a commit failed validation
     *
     * @param cause validation failure
     */
    private def rebase(cause: Throwable): Unit = {
      val previous = pinned().snapshot.map(_.snapshotId())
      val current = pin()
      if (current.snapshot.map(_.snapshotId()) == previous) {
        // the table hasn't changed, so the failure wasn't caused by a concurrent operation and will just happen again
        throw cause
      }
      logger.debug(s"Rebasing transaction from snapshot ${previous.orNull} to ${current.snapshot.map(_.snapshotId()).orNull}")
      deletes.foreach { case (path, pending) =>
        pending.positions.forEach((position: Long) => validate(current, path, position, cause))
      }
      view = current
    }

    /**
     * Writes out the pending deletes as deletion vectors, merged with any existing deletes for the same files
     *
     * @param paths any files created will be added to this buffer
     * @return
     */
    private def writeDeletes(paths: ArrayBuffer[String]): DeleteWriteResult = {
      val io = new FileIO() {
        override def newInputFile(path: String): InputFile = table.io().newInputFile(path)
        override def newOutputFile(path: String): OutputFile = { paths += path; table.io().newOutputFile(path) }
        override def deleteFile(path: String): Unit = table.io().deleteFile(path)
      }
      val files = OutputFileFactory.builderFor(table, 0, 0L).format(FileFormat.PUFFIN).ioSupplier(() => io).build()
      val writer = new BaseDVFileWriter(files, pinned())
      deletes.foreach { case (path, pending) => writer.delete(path, pending.positions, pending.spec, pending.partition) }
      // note: nothing is written until the writer is closed
      writer.close()
      writer.result()
    }

    private def isConflict(e: Throwable): Boolean = e match {
      case _: ValidationException => true
      // thrown if our starting snapshot is no longer part of the table history, i.e. due to a rollback or expiration
      case _: IllegalArgumentException => e.getMessage != null && e.getMessage.contains("is not an ancestor of")
      case _ => false
    }

    private def checkOpen(): Unit = {
      if (finished != null) {
        throw new IllegalStateException(s"Transaction has already been $finished")
      }
    }

    /**
     * Marks the transaction as finished, and releases any state
     *
     * @param how description of how the transaction finished
     */
    private def finish(how: String): Unit = {
      finished = how
      dataFiles.clear()
      deletes.clear()
      callbacks.clear()
      view = null
    }

    private def pinned(): View = {
      if (view == null) {
        view = pin()
      }
      view
    }

    private def pin(): View = {
      // the table is generally long-lived, so make sure that we're working against the latest state
      table.refresh()
      new View(Option(table.currentSnapshot()))
    }

    /**
     * Deletes a file that isn't part of the table, logging any failure
     *
     * @param path file path
     */
    private[core] def deleteQuietly(path: String): Unit = {
      try { table.io().deleteFile(path) } catch {
        case NonFatal(e) => logger.warn(s"Error deleting file, it will need to be cleaned up as an orphan: $path", e)
      }
    }

    /**
     * View of the table at a given snapshot. Loads existing deletes for a given data file path, so they can be merged
     * into a newly written DV - returns null if there are no existing deletes for the given path.
     *
     * @param snapshot snapshot, if the table has one
     */
    private class View(val snapshot: Option[Snapshot]) extends java.util.function.Function[String, PositionDeleteIndex] {

      // map of data-file-path -> spec id and partition. if partitioning is changed, old files might have different partition specs
      private lazy val dataFilesByPath: CharSequenceMap[(Int, StructLike)] = {
        val map = CharSequenceMap.create[(Int, StructLike)]()
        snapshot.foreach { snapshot =>
          val io = table.io()
          val specs = table.specs()
          snapshot.dataManifests(io).asScala.foreach { manifest =>
            WithClose(ManifestFiles.read(manifest, io, specs)) { reader =>
              WithClose(reader.iterator()) { iterator =>
                // manifest readers reuse record instances
                iterator.asScala.foreach(file => map.put(file.location(), (file.specId(), file.copy().partition())))
              }
            }
          }
        }
        map
      }

      // map of data-file-path -> set of delete files that reference it
      private lazy val deletesByPath: CharSequenceMap[DeleteFileSet] = {
        val map = CharSequenceMap.create[DeleteFileSet]()
        snapshot.foreach { snapshot =>
          val io = table.io()
          val specs = table.specs()
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

      private lazy val loader: DeleteLoader = new BaseDeleteLoader(deleteFile => table.io().newInputFile(deleteFile))

      // the deletes are checked for each deleted row, and again when writing the new DVs, so hold on to them
      private val loaded = scala.collection.mutable.Map.empty[String, PositionDeleteIndex]

      /**
       * Gets the partition spec id and partition for a data file
       *
       * @param path data file path
       * @return null if the data file is not part of the table
       */
      def dataFile(path: String): (Int, StructLike) = dataFilesByPath.get(path)

      /**
       * Gets existing deletes for a data file
       *
       * @param path data file path
       * @return null if there are no deletes for the file
       */
      def previousDeletes(path: String): PositionDeleteIndex = {
        loaded.getOrElseUpdate(path, {
          val set = deletesByPath.get(path)
          if (set == null) { null } else { loader.loadPositionDeletes(set, path) }
        })
      }

      override def apply(path: String): PositionDeleteIndex = previousDeletes(path)
    }
  }

  /**
   * Indicates that a change can't be made because it conflicts with another operation - either a concurrent
   * operation that modified the same rows or removed the files they are in, or an earlier modification of the same
   * row in the same transaction. The transaction should be rolled back, and may be re-tried.
   *
   * Extends IOException so that it is passed through as-is by the GeoTools API.
   *
   * @param message message
   * @param cause cause
   */
  class ConflictException(message: String, cause: Throwable) extends IOException(message, cause)

  /**
   * Rows deleted out of a single data file
   *
   * @param spec partition spec of the data file
   * @param partition partition of the data file
   * @param positions deleted row positions
   */
  private case class PendingDeletes(spec: PartitionSpec, partition: StructLike, positions: PositionDeleteIndex)
}
