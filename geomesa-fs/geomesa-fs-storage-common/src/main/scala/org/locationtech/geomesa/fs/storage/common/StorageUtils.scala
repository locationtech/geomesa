/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme

import scala.collection.mutable

object StorageUtils {

  def partitionsAndFiles(root: Path,
                         fs: FileSystem,
                         typeName: String,
                         partitionScheme: PartitionScheme,
                         fileSequenceLength: Int,
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
    dataFiles.map { f =>
      if (isLeaf) {
        val prefixToRemove = typePath.toUri.getPath + "/"
        val partition = f.toUri.getPath.dropRight(fileSequenceLength + 1 + fileExtension.length).replaceAllLiterally(prefixToRemove, "")
        val file = f.getName
        (partition, file)
      } else {
        val prefixToRemove = typePath.toUri.getPath + "/"
        (f.getParent.toUri.getPath.replaceAllLiterally(prefixToRemove, ""), f.getName)
      }
    }.groupBy(_._1).map { case (k, iter) =>
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
