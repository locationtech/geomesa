/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.util.UUID

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.FileType.FileType

import scala.collection.mutable

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

  def partitionsAndFiles(root: Path,
                         fs: FileSystem,
                         typeName: String,
                         partitionScheme: PartitionScheme,
                         fileExtension: String): java.util.Map[String, java.util.List[String]] = {
    val typePath = new Path(root, typeName)
    val files = fs.listFiles(typePath, true)
    val dataFiles = mutable.ListBuffer.empty[Path]
    while (files.hasNext) {
      val cur = files.next().getPath
      if (cur.getName.endsWith(fileExtension)) {
        dataFiles += cur
      }
    }
    val isLeaf = partitionScheme.isLeafStorage

    import scala.collection.JavaConversions._
    dataFiles.map(f => (getPartition(typePath, f, isLeaf, fileExtension), f.getName))
      .groupBy(_._1).map { case (k, iter) =>
      import scala.collection.JavaConverters._
      k -> iter.map(_._2).toList.asJava
    }
  }

  def listFiles(fs: FileSystem, dir: Path, ext: String): Seq[Path] = {
    if (fs.exists(dir)) {
      fs.listStatus(dir).map { f => f.getPath }.filter(_.getName.endsWith(ext)).toSeq
    } else {
      Seq.empty[Path]
    }
  }

  def listFiles(fs: FileSystem,
                root: Path,
                typeName: String,
                partition: String,
                isLeafStorage: Boolean,
                ext: String): Seq[Path] = {
    val pp = partitionPath(root, typeName, partition)
    val dir = if (isLeafStorage) pp.getParent else pp
    val files = listFiles(fs, dir, ext)
    if (isLeafStorage) files.filter(_.getName.startsWith(partition.split('/').last)) else files
  }

  def listFileStatuses(fs: FileSystem, dir: Path, ext: String): Seq[FileStatus] = {
    if (fs.exists(dir)) {
      fs.listStatus(dir).filter(_.getPath.getName.endsWith(ext)).toSeq
    } else {
      Seq.empty[FileStatus]
    }
  }

  def listFileStatus(fs: FileSystem,
                     root: Path,
                     typeName: String,
                     partition: String,
                     ext: String,
                     isLeafStorage: Boolean): Seq[FileStatus] = {
    val pp = partitionPath(root, typeName, partition)
    val dir = if (isLeafStorage) pp.getParent else pp
    val files = listFileStatuses(fs, dir, ext)
    if (isLeafStorage) files.filter(_.getPath.getName.startsWith(partition.split('/').last)) else files
  }

  def partitionPath(root: Path, typeName: String, partitionName: String): Path =
    new Path(new Path(root, typeName), partitionName)

  private def randomName: String = UUID.randomUUID().toString.replaceAllLiterally("-", "")
  def createLeafName(prefix: String, ext: String, fileType: FileType): String = f"${prefix}_$fileType$randomName.$ext"
  def createBucketName(ext: String, fileType: FileType): String = f"$fileType$randomName.$ext"

  def nextFile(fs: FileSystem,
               root: Path,
               typeName: String,
               partitionName: String,
               isLeafStorage: Boolean,
               extension: String,
               fileType: FileType): Path = {

    val components = partitionName.split('/')
    val baseFileName = components.last

    if (isLeafStorage) {
      val dir = partitionPath(root, typeName, partitionName).getParent
      val name = createLeafName(baseFileName, extension, fileType)
      new Path(dir, name)
    } else {
      val dir = partitionPath(root, typeName, partitionName)
      val name = createBucketName(extension, fileType)
      new Path(dir, name)
    }
  }

}

object FileType extends Enumeration {
  type FileType = Value
  val Written   = Value("W")
  val Compacted = Value("C")
  val Imported  = Value("I")
}