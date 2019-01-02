/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import java.io.InputStream

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.HadoopDelegate.HadoopFileHandle

/**
  * Delegate allows us to avoid a runtime dependency on hadoop
  */
class HadoopDelegate extends FileSystemDelegate {

  import HadoopDelegate.HiddenFileFilter
  import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus, Path}

  private val conf = new org.apache.hadoop.conf.Configuration()
  // use the same property as FileInputFormat
  private val recursive = conf.getBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false)

  // based on logic from hadoop FileInputFormat
  override def interpretPath(path: String): Seq[FileHandle] = {
    val p = new Path(path)
    // TODO close filesystem?
    val fs = p.getFileSystem(conf)
    val files = fs.globStatus(p, HiddenFileFilter)

    if (files == null) {
      throw new IllegalArgumentException(s"Input path does not exist: $path")
    } else if (files.isEmpty) {
      throw new IllegalArgumentException(s"Input path does not match any files: $path")
    }

    def getFiles(file: FileStatus): Seq[FileHandle] = {
      if (!file.isDirectory) {
        Seq(new HadoopFileHandle(file, fs))
      } else if (recursive) {
        val children = fs.listLocatedStatus(file.getPath)
        new Iterator[LocatedFileStatus] {
          override def hasNext: Boolean = children.hasNext
          override def next(): LocatedFileStatus = children.next
        }.filter(f => HiddenFileFilter.accept(f.getPath)).toSeq.flatMap(getFiles)
      } else {
        Seq.empty
      }
    }

    files.flatMap(getFiles)
  }
}

object HadoopDelegate {

  val HiddenFileFilter: PathFilter = new PathFilter() {
    override def accept(path: Path): Boolean = {
      val name = path.getName
      !name.startsWith("_") && !name.startsWith(".")
    }
  }

  class HadoopFileHandle(file: FileStatus, fs: FileSystem) extends FileHandle {
    override def path: String = file.getPath.toString
    override def length: Long = file.getLen
    override def open: InputStream = fs.open(file.getPath)
  }
}
