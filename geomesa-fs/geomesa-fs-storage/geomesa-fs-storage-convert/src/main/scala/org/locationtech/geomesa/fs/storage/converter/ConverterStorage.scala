/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import org.locationtech.jts.geom.Envelope
import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.Query
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.fs.storage.converter.ConverterStorage.ConverterMetadata
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ConverterStorage(fc: FileContext,
                       root: Path,
                       sft: SimpleFeatureType,
                       converter: SimpleFeatureConverter,
                       partitionScheme: PartitionScheme) extends FileSystemStorage {

  // TODO close converter...
  // the problem is that we aggressively cache storage instances for performance (in FileSystemStorageManager),
  // so even if we wired a 'close' method through the entire storage api, we'd also have to implement a
  // 'borrow/return' paradigm and expire idle instances. Since currently only converters with redis caches
  // actually need to be closed, and since they will only open a single connection per converter, the
  // impact should be low

  private val metadata = new ConverterMetadata(fc, root, sft, partitionScheme)

  import scala.collection.JavaConverters._

  override def getMetadata: StorageMetadata = metadata

  override def getPartitions: java.util.List[PartitionMetadata] = metadata.getPartitions

  override def getPartitions(filter: Filter): java.util.List[PartitionMetadata] = {
    val all = getPartitions
    if (filter == Filter.INCLUDE) { all } else {
      val coveringPartitions = new java.util.HashSet(metadata.getPartitionScheme.getPartitions(filter))
      if (!coveringPartitions.isEmpty) {
        val iter = all.iterator()
        while (iter.hasNext) {
          if (!coveringPartitions.contains(iter.next.name)) {
            iter.remove()
          }
        }
      }
      all
    }
  }

  override def getPartition(feature: SimpleFeature): String = partitionScheme.getPartition(feature)

  override def getReader(partitions: java.util.List[String], query: Query): FileSystemReader =
    getReader(partitions, query, 1)

  override def getReader(partitions: java.util.List[String], query: Query, threads: Int): FileSystemReader =
    new ConverterPartitionReader(this, partitions.asScala, converter, query.getFilter)

  override def getFilePaths(partition: String): java.util.List[Path] = {
    val path = new Path(root, partition)
    if (partitionScheme.isLeafStorage) { Collections.singletonList(path) } else {
      PathCache.list(fc, path).map(_.getPath).toList.asJava
    }
  }

  override def getWriter(partition: String): FileSystemWriter =
    throw new UnsupportedOperationException("Converter storage does not support feature writing")

  override def compact(partition: String): Unit =
    throw new UnsupportedOperationException("Converter storage does not support compactions")

  override def compact(partition: String, threads: Int): Unit =
    throw new UnsupportedOperationException("Converter storage does not support compactions")
}

object ConverterStorage {

  val Encoding = "converter"

  class ConverterMetadata(fc: FileContext,
                          root: Path,
                          sft: SimpleFeatureType,
                          scheme: PartitionScheme) extends StorageMetadata {

    import scala.collection.JavaConverters._

    private [ConverterStorage] val dirty = new AtomicBoolean(false)

    override def getRoot: Path = root

    override def getFileContext: FileContext = fc

    override def getEncoding: String = ConverterStorage.Encoding

    override def getPartitionScheme: PartitionScheme = scheme

    override def getSchema: SimpleFeatureType = sft

    override def getPartition(name: String): PartitionMetadata = {
      val path = new Path(name)
      val files = if (!PathCache.exists(fc, path)) { Collections.emptyList[String]() } else {
        PathCache.list(fc, path).map(_.getPath.getName).toList.asJava
      }
      new PartitionMetadata(name, files, -1L, new Envelope())
    }

    override def getPartitions: java.util.List[PartitionMetadata] =
      buildPartitionList(fc, scheme, root, "", 0, dirty.compareAndSet(true, false)).map(getPartition).asJava

    override def addPartition(partition: PartitionMetadata): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def removePartition(partition: PartitionMetadata): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def compact(): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def reload(): Unit = dirty.set(true)
  }

  private def buildPartitionList(fc: FileContext,
                                 scheme: PartitionScheme,
                                 path: Path,
                                 prefix: String,
                                 curDepth: Int,
                                 invalidate: Boolean): List[String] = {
    if (invalidate) {
      PathCache.invalidate(fc, path)
    }
    if (curDepth == 0) {
      PathCache.list(fc, path).flatMap(f => buildPartitionList(fc, scheme, f.getPath, prefix, curDepth + 1, invalidate)).toList
    } else if (curDepth > scheme.getMaxDepth || !PathCache.status(fc, path).isDirectory) {
      List(s"$prefix${path.getName}")
    } else {
      val next = s"$prefix${path.getName}/"
      PathCache.list(fc, path).flatMap(f => buildPartitionList(fc, scheme, f.getPath, next, curDepth + 1, invalidate)).toList
    }
  }
}