/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.codec.digest.MurmurHash3
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage._
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.utils.FileSystemThreadedReader
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.utils.SortingSimpleFeatureIterator
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.io.{Closeable, Flushable}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID

/**
 * Persists simple features to a file system and provides query access. Storage implementations are fairly
 * lightweight, in that all state is captured in the metadata instance
 */
trait FileSystemStorage extends Closeable with StrictLogging {

  val fs: ObjectStore = ObjectStore(context)

  /**
   * Handle to the file context, root path and configuration
   *
   * @return
   */
  def context: FileSystemContext

  /**
   * Metadata on files for this instance
   *
   * @return
   */
  def metadata: StorageMetadata

  /**
   * File encoding used by this instance
   *
   * @return
   */
  def encoding: String

  /**
    * Get a reader for all relevant partitions
    *
    * @param query query
    * @param threads suggested threads used for reading data files
    * @return reader
    */
  def getReader(query: Query, threads: Int): CloseableFeatureIterator = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val configured = QueryRunner.configureQuery(metadata.sft, query)
    val filter = Option(configured.getFilter).getOrElse(Filter.INCLUDE)
    val transform = configured.getHints.getTransform
    val sort = configured.getHints.getSortFields
    val max = configured.getHints.getMaxFeatures

    logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(filter)}")
    logger.debug(s"  Original filter: ${ECQL.toCQL(query.getFilter)}")
    logger.debug(s"  Transforms: ${transform.fold("none") { case (t, _) => if (t.isEmpty) { "empty" } else { t }}}")
    logger.debug(s"  Sort: ${sort.fold("none") { fields => fields.map { case (f, rev) => s"$f ${if (rev) "descending" else ""}"}.mkString(", ")}}")
    logger.debug(s"  Max features: ${max.getOrElse("none")}")

    val files = metadata.getFiles(filter)
    logger.debug(s"  Threading the read of ${files.size} files with $threads reader threads")
    logger.whenTraceEnabled(files.foreach(f => logger.trace(s"    $f")))

    if (files.isEmpty) {
      CloseableIterator.empty
    } else {
      val reader = createReader(Option(filter).filterNot(_ == Filter.INCLUDE), transform)
      val threaded = FileSystemThreadedReader(reader, files, threads)
      val sorted = sort.fold(threaded)(s => new SortingSimpleFeatureIterator(threaded, s))
      val limited = max.fold(sorted)(m => sorted.take(m))
      limited
    }
  }

  /**
    * Get a writer for a given partition. This method is thread-safe and can be called multiple times,
    * although this can result in multiple data files.
    *
    * @param partition partitions
    * @return writer
    */
  def getWriter(partition: Partition): FileSystemWriter

  /**
    * Gets a modifying writer. This method is thread-safe and can be called multiple times,
    * although if a feature is modified multiple times concurrently, the last update 'wins'.
    * There is no guarantee that any concurrent modifications will be reflected in the returned
    * writer.
    *
    * @param filter the filter used to select features for modification
    * @param threads suggested threads used for reading data files
    * @return
    */
  def getWriter(filter: Filter, threads: Int): FileSystemUpdateWriter

  /**
   * Create a file reader
   *
   * @param filter filter
   * @param transform transform
   * @return
   */
  protected def createReader(filter: Option[Filter], transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader
}

object FileSystemStorage {

  private final val SafeNameRegex = "[^a-zA-Z0-9_-]+".r

  /**
   * Get the path for a new data file
   *
   * @param ext file extension
   * @param fileType file type
   * @return
   */
  def newFilePath(typeName: String, fileType: FileType.FileType, ext: String): String = {
    val filename =
      s"${fileType}_${SafeNameRegex.replaceAllIn(typeName, "-").take(20)}_${UUID.randomUUID().toString.replaceAllLiterally("-", "")}.$ext"
    // partitioning logic taken from Apache Iceberg: https://iceberg.apache.org/docs/nightly/aws/#object-store-file-layout
    val hash = {
      val bytes = filename.getBytes(StandardCharsets.UTF_8)
      val hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, 0)
      // Integer#toBinaryString excludes leading zeros, which we want to preserve
      Integer.toBinaryString(hash | Integer.MIN_VALUE)
    }
    s"${hash.substring(0, 4)}/${hash.substring(4, 8)}/${hash.substring(8, 12)}/${hash.substring(12, 20)}/$filename"
  }

  object FileType extends Enumeration {
    type FileType = Value
    val Written  : Value = Value("w")
    val Compacted: Value = Value("c")
    val Modified : Value = Value("m")
    val Deleted  : Value = Value("d")
  }

  /**
   * Append writer
   */
  trait FileSystemWriter extends Closeable with Flushable {

    /**
      * Write a feature
      *
      * @param feature feature
      */
    def write(feature: SimpleFeature): Unit

    /**
     * Gets the size of the data written so far, in bytes. May not be accurate until the writer is
     * closed, due to buffering, etc
     *
     * @return
     */
    def size: Long
  }

  /**
   * Update writer
   *
   */
  trait FileSystemUpdateWriter extends Iterator[SimpleFeature] with Closeable with Flushable {

    /**
     * Writes a modification to the last feature returned by `next`
     */
    def write(): Unit

    /**
     * Deletes the last feature returned by `next`
     */
    def remove(): Unit
  }

  /**
   * Reader trait
   */
  trait FileSystemPathReader {

    /**
     * Root path
     *
     * @return
     */
    def root: URI

    /**
     * Reads a file
     *
     * @param file file, relative to the root path
     * @return
     */
    def read(file: URI): Iterator[SimpleFeature] with Closeable
  }
}
