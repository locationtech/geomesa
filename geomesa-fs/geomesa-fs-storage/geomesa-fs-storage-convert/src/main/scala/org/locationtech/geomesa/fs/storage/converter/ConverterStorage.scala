/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.net.URI
import java.util.Collections

import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.Query
import org.locationtech.geomesa.convert.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class ConverterStorage(root: Path,
                       fs: FileSystem,
                       partitionScheme: PartitionScheme,
                       sft: SimpleFeatureType,
                       converter: SimpleFeatureConverter[_]) extends FileSystemStorage {
  import scala.collection.JavaConversions._

  override def getRoot: URI = root.toUri

  override def getTypeNames: java.util.List[String] = Collections.singletonList(sft.getTypeName)

  override def getFeatureTypes: java.util.List[SimpleFeatureType] = Collections.singletonList(sft)

  override def getFeatureType(name: String): SimpleFeatureType = {
    if (sft.getTypeName != name) {
      throw new IllegalArgumentException(s"Type $name doesn't match configured sft name ${sft.getTypeName}")
    }
    sft
  }

  override def getReader(typeName: String,
                         partitions: java.util.List[String],
                         query: Query): FileSystemReader = getReader(typeName, partitions, query, 1)

  override def getReader(typeName: String,
                         partitions: java.util.List[String],
                         query: Query,
                         threads: Int): FileSystemReader = {
    new ConverterPartitionReader(root, partitions, sft, converter, query.getFilter)
  }

  override def getPartitionScheme(typeName: String): PartitionScheme = partitionScheme

  override def getPartitions(typeName: String): java.util.List[String] = buildPartitionList(root, "", 0)

  override def getPartitions(typeName: String, query: Query): java.util.List[String] = {
    val all = getPartitions(typeName)
    if (query.getFilter == Filter.INCLUDE) { all } else {
      val coveringPartitions = partitionScheme.getCoveringPartitions(query.getFilter)
      if (coveringPartitions.isEmpty) {
        all //TODO should this ever happen?
      } else {
        all.intersect(coveringPartitions)
      }
    }
  }

  override def getPaths(typeName: String, partition: String): java.util.List[URI] =
    List(new Path(root, partition).toUri)

  private def buildPartitionList(path: Path, prefix: String, curDepth: Int): List[String] = {
    if (curDepth > partitionScheme.maxDepth()) {
      return List.empty[String]
    }
    val status = fs.listStatus(path)
    status.flatMap { f =>
      if (f.isDirectory) {
        buildPartitionList(f.getPath, s"$prefix${f.getPath.getName}/", curDepth + 1)
      } else if (f.getPath.getName.equals("schema.sft")) {
        List.empty
      } else {
        List(s"$prefix${f.getPath.getName}")
      }
    }.toList
  }

  override def createNewFeatureType(sft: SimpleFeatureType, partitionScheme: PartitionScheme): Unit =
    throw new UnsupportedOperationException("Converter Storage does not support creation of new feature types")

  override def getWriter(typeName: String, partition: String): FileSystemWriter =
    throw new UnsupportedOperationException("Converter Storage does not support feature writing")

  override def getMetadata(typeName: String): Metadata =
    throw new UnsupportedOperationException("Cannot append to converter datastore")

  override def updateMetadata(typeName: String): Unit =
    throw new UnsupportedOperationException("Cannot append to converter datastore")

  override def compact(typeName: String, partition: String): Unit =
    throw new UnsupportedOperationException("Converter datastore does not support compactions")

  override def compact(typeName: String, partition: String, threads: Int): Unit =
    throw new UnsupportedOperationException("Converter datastore does not support compactions")
}
