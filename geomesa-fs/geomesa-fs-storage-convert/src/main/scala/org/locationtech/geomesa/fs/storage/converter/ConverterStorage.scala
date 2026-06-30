/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import org.apache.iceberg.Table
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.converter.pathfilter.PathFiltering
import org.locationtech.geomesa.fs.storage.converter.schemes.PartitionScheme
import org.locationtech.geomesa.fs.storage.converter.utils.FileSystemThreadedReader
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.{FileSystemUpdateWriter, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.iceberg.SimpleFeatureIcebergSchema
import org.locationtech.geomesa.fs.storage.core.{CacheDurationProperty, FileSystemContext, FileSystemStorage, Partition}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.utils.SortingSimpleFeatureIterator
import org.locationtech.geomesa.security.VisibilityUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.net.URI
import java.util.concurrent.TimeUnit
import scala.runtime.BoxedUnit
import scala.util.control.NonFatal

class ConverterStorage(
    context: FileSystemContext,
    table: Table,
    schema: SimpleFeatureIcebergSchema,
    layout: PartitionScheme,
    converter: SimpleFeatureConverter,
    pathFiltering: Option[PathFiltering],
    leafStorage: Boolean,
  ) extends FileSystemStorage(context, table, Seq.empty, schema) {

  import scala.collection.JavaConverters._

  private val fs = ObjectStore(context)

  private val depth = layout.depth - (if (leafStorage) { 1 } else { 0 })

  private val filesCache: LoadingCache[BoxedUnit, Map[String, Seq[URI]]] =
    Caffeine.newBuilder().refreshAfterWrite(CacheDurationProperty.toDuration.get.toMillis, TimeUnit.MILLISECONDS).build(
      new CacheLoader[BoxedUnit, Map[String, Seq[URI]]]() {
        override def load(key: BoxedUnit): Map[String, Seq[URI]] = buildFileList()
      }
    )

  override def getReader(query: Query, threads: Int): CloseableIterator[SimpleFeature] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val configured = QueryRunner.configureQuery(sft, query)
    val filter = Option(configured.getFilter)
    val visFilter = VisibilityUtils.visible(authProvider)
    val transform = configured.getHints.getTransform
    val sort = configured.getHints.getSortFields
    val max = configured.getHints.getMaxFeatures

    logger.debug(s"Running query '${query.getTypeName}' ${ECQL.toCQL(filter.getOrElse(Filter.INCLUDE))}")
    logger.debug(s"  Original filter: ${ECQL.toCQL(query.getFilter)}")
    logger.debug(s"  Transforms: ${transform.fold("none") { case (t, _) => if (t.isEmpty) { "empty" } else { t }}}")
    logger.debug(s"  Sort: ${sort.fold("none") { fields => fields.map { case (f, rev) => s"$f ${if (rev) "descending" else ""}"}.mkString(", ")}}")
    logger.debug(s"  Max features: ${max.getOrElse("none")}")

    val files = filter.flatMap(layout.getIntersectingPartitions) match {
      case None => filesCache.get(BoxedUnit.UNIT).flatMap(_._2)
      case Some(partitions) => filesCache.get(BoxedUnit.UNIT).flatMap { case (partition, uris) =>
        if (partitions.contains(partition)) { uris } else { Seq.empty }
      }
    }

    val reader = new ConverterFileSystemReader(fs, context.root, converter, filter, transform, pathFiltering)
    val scan = FileSystemThreadedReader(reader, files.toSeq, threads)

    try {
      val iter = scan.filter(visFilter.apply)
      val sorted = sort.fold(iter)(s => new SortingSimpleFeatureIterator(iter, s))
      val limited = max.fold(sorted)(m => sorted.take(m))
      limited
    } catch {
      case NonFatal(e) => CloseWithLogging(scan); throw e
    }
  }

  override def getWriter(partition: Partition): FileSystemWriter =
    throw new UnsupportedOperationException("Converter storage is read-only")

  override def getWriter(filter: Filter, threads: Int): FileSystemUpdateWriter =
    throw new UnsupportedOperationException("Converter storage is read-only")

  override def close(): Unit = {
    try { super.close() } finally {
      CloseWithLogging(Seq(fs, converter))
    }
  }

  private def buildFileList(): Map[String, Seq[URI]] = {
    logger.debug("Building file list")
    val start = System.currentTimeMillis()
    val map = new java.util.HashMap[String, Seq[URI]]()
    try {
      val toCheck = new java.util.LinkedList[URI]()
      fs.list(context.root).foreach(toCheck.add)
      val maxDepth = depth + context.root.toString.count(_ == '/')
      val rootPathLength = context.root.toString.length
      while (!toCheck.isEmpty) {
        val file = toCheck.remove()
        logger.debug(s"Checking ${file.getPath}")
        if (file.getPath.endsWith("/")) {
          if (file.toString.count(_ == '/') <= maxDepth) {
            fs.list(file).foreach(toCheck.add)
          }
        } else if (!fs.filename(file).startsWith(".")) { // ignore "hidden" files
          val relativePath = file.toString.substring(rootPathLength)
          var partitionParts = relativePath.split("/")
          if (leafStorage) {
            partitionParts(partitionParts.length - 1) = partitionParts.last.takeWhile(_ != '_')
          } else {
            partitionParts = partitionParts.dropRight(1)
          }
          val partition = partitionParts.mkString("/")
          map.put(partition, map.getOrDefault(partition, Seq.empty) :+ file)
        }
      }
    } catch {
      case NonFatal(e) => logger.error("Error building partition list:", e); throw e
    }
    logger.debug(s"Found ${map.size} partitions in ${System.currentTimeMillis() - start}ms")
    logger.whenTraceEnabled(map.asScala.values.foreach(uris => uris.foreach(uri => logger.trace(uri.toString))))
    map.asScala.toMap
  }
}
