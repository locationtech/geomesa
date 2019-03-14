/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import java.io.InputStream
import java.util.Locale

import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.HadoopDelegate.{HadoopFileHandle, HadoopTarHandle, HadoopZipHandle}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import scala.collection.mutable.ListBuffer

/**
  * Delegate allows us to avoid a runtime dependency on hadoop
  */
class HadoopDelegate extends FileSystemDelegate {

  import HadoopDelegate.HiddenFileFilter
  import org.apache.hadoop.fs.{LocatedFileStatus, Path}

  private val conf = new org.apache.hadoop.conf.Configuration()
  // use the same property as FileInputFormat
  private val recursive = conf.getBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false)

  // based on logic from hadoop FileInputFormat
  override def interpretPath(path: String): Seq[FileHandle] = {
    import ArchiveStreamFactory.{JAR, TAR, ZIP}

    val p = new Path(path)
    // TODO close filesystem?
    val fs = p.getFileSystem(conf)
    val files = fs.globStatus(p, HiddenFileFilter)

    if (files == null) {
      throw new IllegalArgumentException(s"Input path does not exist: $path")
    } else if (files.isEmpty) {
      throw new IllegalArgumentException(s"Input path does not match any files: $path")
    }

    val remaining = scala.collection.mutable.Queue(files: _*)
    val result = ListBuffer.empty[FileHandle]

    while (remaining.nonEmpty) {
      val file = remaining.dequeue()
      if (file.isDirectory) {
        if (recursive) {
          val children = fs.listLocatedStatus(file.getPath)
          val iter = new Iterator[LocatedFileStatus] {
            override def hasNext: Boolean = children.hasNext
            override def next(): LocatedFileStatus = children.next
          }
          remaining ++= iter.filter(f => HiddenFileFilter.accept(f.getPath))
        }
      } else {
        PathUtils.getUncompressedExtension(file.getPath.getName).toLowerCase(Locale.US) match {
          case TAR       => result += new HadoopTarHandle(file, fs)
          case ZIP | JAR => result += new HadoopZipHandle(file, fs)
          case _         => result += new HadoopFileHandle(file, fs)
        }
      }
    }

    result.result
  }
}

object HadoopDelegate {

  private val factory = new ArchiveStreamFactory()

  val HiddenFileFilter: PathFilter = new PathFilter() {
    override def accept(path: Path): Boolean = {
      val name = path.getName
      !name.startsWith("_") && !name.startsWith(".")
    }
  }

  class HadoopFileHandle(file: FileStatus, fs: FileSystem) extends FileHandle {
    override def path: String = file.getPath.toString
    override def length: Long = file.getLen
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val is = PathUtils.handleCompression(fs.open(file.getPath), file.getPath.getName)
      CloseableIterator.single(None -> is, is.close())
    }
  }

  class HadoopZipHandle(file: FileStatus, fs: FileSystem) extends HadoopFileHandle(file, fs) {
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      // we have to read the bytes into memory to get random access reads
      val bytes = WithClose(PathUtils.handleCompression(fs.open(file.getPath), file.getPath.getName)) { is =>
        IOUtils.toByteArray(is)
      }
      new ZipFileIterator(new ZipFile(new SeekableInMemoryByteChannel(bytes)), file.getPath.toString)
    }
  }

  class HadoopTarHandle(file: FileStatus, fs: FileSystem) extends HadoopFileHandle(file, fs) {
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val uncompressed = PathUtils.handleCompression(fs.open(file.getPath), file.getPath.getName)
      val archive = factory.createArchiveInputStream(ArchiveStreamFactory.TAR, uncompressed)
      new ArchiveFileIterator(archive, file.getPath.toString)
    }
  }
}
