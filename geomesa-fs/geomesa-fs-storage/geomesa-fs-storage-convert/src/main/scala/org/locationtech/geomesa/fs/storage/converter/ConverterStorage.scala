/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.Query
import org.locationtech.geomesa.convert.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.fs.storage.converter.ConverterStorage.ConverterMetadata
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ConverterStorage(fc: FileContext,
                       root: Path,
                       sft: SimpleFeatureType,
                       converter: SimpleFeatureConverter[_],
                       partitionScheme: PartitionScheme) extends FileSystemStorage {

  private val metadata = new ConverterMetadata(fc, root, sft, partitionScheme)

  import scala.collection.JavaConverters._

  override def getMetadata: FileMetadata = metadata

  override def getPartitions: java.util.List[String] = metadata.getPartitions

  override def getPartitions(filter: Filter): java.util.List[String] = {
    val all = getPartitions
    lazy val coveringPartitions = partitionScheme.getPartitions(filter)
    if (filter == Filter.INCLUDE || coveringPartitions.isEmpty) { all } else { // TODO should this ever happen?
      val filtered = new java.util.ArrayList(all)
      filtered.retainAll(coveringPartitions)
      filtered
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

  override def updateMetadata(): Unit = metadata.dirty.set(true)
}

object ConverterStorage {

  val Encoding = "converter"

  class ConverterMetadata(fc: FileContext,
                          root: Path,
                          sft: SimpleFeatureType,
                          scheme: PartitionScheme) extends FileMetadata {

    import scala.collection.JavaConverters._

    private [ConverterStorage] val dirty = new AtomicBoolean(false)

    override def getRoot: Path = root

    override def getFileContext: FileContext = fc

    override def getEncoding: String = ConverterStorage.Encoding

    override def getPartitionScheme: PartitionScheme = scheme

    override def getSchema: SimpleFeatureType = sft

    override def getPartitionCount: Int = getPartitions.size()

    override def getFileCount: Int =
      getPartitions.asScala.map(p => PathCache.list(fc, new Path(p)).length).sum

    override def getPartitions: java.util.List[String] =
      buildPartitionList(fc, scheme, root, "", 0, dirty.compareAndSet(true, false)).asJava

    override def getFiles(partition: String): java.util.List[String] =
      PathCache.list(fc, new Path(partition)).map(_.getPath.getName).toList.asJava

    override def getPartitionFiles: java.util.Map[String, java.util.List[String]] =
      getPartitions.asScala.map(p => p -> getFiles(p)).toMap.asJava

    override def addFile(partition: String, file: String): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def addFiles(partition: String, files: java.util.List[String]): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def addFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def removeFile(partition: String, file: String): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def removeFiles(partition: String, file: java.util.List[String]): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def removeFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def replaceFiles(partition: String, files: java.util.List[String], replacement: String): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")

    override def setFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit =
      throw new UnsupportedOperationException("Converter storage does not support updating metadata")
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