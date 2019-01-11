/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.utils

import java.util.UUID

import org.apache.hadoop.fs.{FileContext, Path, RemoteIterator}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType.FileType

object StorageUtils {

  /**
    * Get the partition name for a datafile path. Datafile contain sequence numbers indicating
    * how they were ingested
    *
    * @param typePath - the path at which this type lives
    * @param filePath - the full path of the data file
    * @param isLeaf - is this a leaf storage file
    * @param fileExtension - the file extension (without a period)
    * @return the partition as a string
    */
  def getPartition(typePath: Path, filePath: Path, isLeaf: Boolean, fileExtension: String): String = {
    if (isLeaf) {
      val pathWithoutExt = filePath.toUri.getPath.dropRight(1 + fileExtension.length)
      val seqNumDropped = pathWithoutExt.substring(0, pathWithoutExt.lastIndexOf('_'))

      val prefixToRemove = typePath.toUri.getPath + "/"
      seqNumDropped.replaceAllLiterally(prefixToRemove, "")
    } else {
      val prefixToRemove = typePath.toUri.getPath + "/"
      filePath.getParent.toUri.getPath.replaceAllLiterally(prefixToRemove, "")
    }
  }

  /**
    * Reads the filesystem to find partitions and the files they contain
    *
    * @param fc file context
    * @param root root storage path
    * @param partitionScheme partition scheme
    * @param fileExtension file extension for data files
    * @return
    */
  def partitionsAndFiles(fc: FileContext,
                         root: Path,
                         partitionScheme: PartitionScheme,
                         fileExtension: String,
                         cache: Boolean = true): java.util.Map[String, java.util.List[String]] = {

    val isLeaf = partitionScheme.isLeafStorage

    val result = new java.util.HashMap[String, java.util.List[String]]

    val newList = new java.util.function.Function[String, java.util.List[String]] {
      override def apply(t: String): java.util.List[String] = new java.util.ArrayList[String]()
    }

    def files(dir: Path): Iterator[Path] = {
      if (!cache) {
        PathCache.invalidate(fc, dir)
      }
      PathCache.list(fc, dir).flatMap { f =>
        val p = f.getPath
        if (f.isDirectory) {
          files(p)
        } else if (p.getName.endsWith(fileExtension)) {
          Iterator.single(p)
        } else {
          Iterator.empty
        }
      }
    }

    files(root).foreach { path =>
      val partition = getPartition(root, path, isLeaf, fileExtension)
      result.computeIfAbsent(partition, newList).add(path.getName)
    }

    result
  }

  /**
    * Gets the path of a partition
    *
    * @param root storage root path
    * @param partition partition
    * @return
    */
  def partitionPath(root: Path, partition: String): Path = new Path(root, partition)

  /**
    * Get the path for a new data file
    *
    * @param root storage root path
    * @param partition partition to write to
    * @param leaf leaf storage or not
    * @param extension file extension
    * @param fileType file type
    * @return
    */
  def nextFile(root: Path,
               partition: String,
               leaf: Boolean,
               extension: String,
               fileType: FileType): Path = {

    val baseFileName = partition.split('/').last

    if (leaf) {
      val dir = partitionPath(root, partition).getParent
      val name = createLeafName(baseFileName, extension, fileType)
      new Path(dir, name)
    } else {
      val dir = partitionPath(root, partition)
      val name = createBucketName(extension, fileType)
      new Path(dir, name)
    }
  }

  private def createLeafName(prefix: String, ext: String, fileType: FileType): String = f"${prefix}_$fileType$randomName.$ext"
  private def createBucketName(ext: String, fileType: FileType): String = f"$fileType$randomName.$ext"

  private def randomName: String = UUID.randomUUID().toString.replaceAllLiterally("-", "")

  object RemoteIterator {
    def apply[T](iter: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): T = iter.next
    }
  }

  object FileType extends Enumeration {
    type FileType = Value
    val Written  : Value = Value("W")
    val Compacted: Value = Value("C")
    val Imported : Value = Value("I")
  }
}
