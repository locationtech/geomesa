/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.fs.storage.api.{Partition, PartitionScheme}

object StorageUtils {

  def buildPartitionList(root: Path,
                         fs: FileSystem,
                         typeName: String,
                         partitionScheme: PartitionScheme,
                         fileSequenceLength: Int,
                         fileExtension: String): List[String] = {

    def recurseBucket(path: Path, prefix: String, curDepth: Int, maxDepth: Int): List[String] = {
      if (curDepth > maxDepth) {
        return List.empty[String]
      }
      val status = fs.listStatus(path)
      status.flatMap { f =>
        if (f.isDirectory) {
          recurseBucket(f.getPath, s"$prefix${f.getPath.getName}/", curDepth + 1, maxDepth)
        } else if (f.getPath.getName.endsWith(s".$fileExtension")) {
          val name = f.getPath.getName.dropRight(fileExtension.length + 1)
          List(prefix.dropRight(1))
        } else {
          List()
        }
      }
    }.toList

    def recurseLeaf(path: Path, prefix: String, curDepth: Int, maxDepth: Int): List[String] = {
      if (curDepth > maxDepth) {
        return List.empty[String]
      }
      val status = fs.listStatus(path)
      status.flatMap { f =>
        if (f.isDirectory) {
          recurseLeaf(f.getPath, s"$prefix${f.getPath.getName}/", curDepth + 1, maxDepth)
        } else if (f.getPath.getName.endsWith(s".$fileExtension")) {
          val lenToDrop = fileSequenceLength + 1 + fileExtension.length
          val name = f.getPath.getName.dropRight(lenToDrop)
          List(s"$prefix$name")
        } else {
          List()
        }
      }
    }.toList

    if (partitionScheme.isLeafStorage) {
      recurseLeaf(new Path(root, typeName), "", 0, partitionScheme.maxDepth())
    } else {
      recurseBucket(new Path(root, typeName), "", 0, partitionScheme.maxDepth() + 1)
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
                partition: Partition,
                isLeafStorage: Boolean,
                ext: String): Seq[Path] = {
    val pp = partitionPath(root, typeName, partition.getName)
    val dir = if (isLeafStorage) pp.getParent else pp
    val files = listFiles(fs, dir, ext)
    if (isLeafStorage) files.filter(_.getName.startsWith(partition.getName.split('/').last)) else files
  }

  def partitionPath(root: Path, typeName: String, partitionName: String): Path =
    new Path(new Path(root, typeName), partitionName)


  val SequenceLength = 5
  def formatLeafFile(prefix: String, i: Int, ext: String): String = f"${prefix}_$i%04d.$ext"
  def formatBucketFile(i: Int, ext: String): String = f"$i%04d.$ext"

  def nextFile(fs: FileSystem,
               root: Path,
               typeName: String,
               partitionName: String,
               isLeafStorage: Boolean,
               extension: String): Path = {

    val components = partitionName.split('/')
    val baseFileName = components.last

    if (isLeafStorage) {
      val dir = partitionPath(root, typeName, partitionName).getParent
      val existingFiles = listFiles(fs, dir, extension).map(_.getName)

      var i = 0
      var name = formatLeafFile(baseFileName, i, extension)
      while (existingFiles.contains(name)) {
        i += 1
        name = formatLeafFile(baseFileName, i, extension)
      }

      new Path(dir, name)
    } else {
      val dir = partitionPath(root, typeName, partitionName)
      val existingFiles = listFiles(fs, dir, extension).map(_.getName)

      var i = 0
      var name = formatBucketFile(i, extension)
      while (existingFiles.contains(name)) {
        i += 1
        name = formatBucketFile(i, extension)
      }

      new Path(dir, name)
    }
  }



}
