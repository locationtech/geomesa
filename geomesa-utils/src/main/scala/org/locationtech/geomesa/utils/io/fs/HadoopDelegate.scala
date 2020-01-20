/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import java.io.{IOException, InputStream, OutputStream}
import java.util.Locale

import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.{CreateMode, FileHandle}
import org.locationtech.geomesa.utils.io.fs.HadoopDelegate.{HadoopFileHandle, HadoopTarHandle, HadoopZipHandle}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import scala.collection.mutable.ListBuffer

/**
  * Delegate allows us to avoid a runtime dependency on hadoop
  */
class HadoopDelegate(conf: Configuration = new Configuration()) extends FileSystemDelegate {

  import ArchiveStreamFactory.{JAR, TAR, ZIP}
  import HadoopDelegate.HiddenFileFilter
  import org.apache.hadoop.fs.{LocatedFileStatus, Path}

  // use the same property as FileInputFormat
  private val recursive = conf.getBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false)

  override def getHandle(path: String): FileHandle = {
    val p = new Path(path)
    val fc = FileContext.getFileContext(p.toUri, conf)
    PathUtils.getUncompressedExtension(p.getName).toLowerCase(Locale.US) match {
      case TAR       => new HadoopTarHandle(fc, p)
      case ZIP | JAR => new HadoopZipHandle(fc, p)
      case _         => new HadoopFileHandle(fc, p)
    }
  }

  // based on logic from hadoop FileInputFormat
  override def interpretPath(path: String): Seq[FileHandle] = {
    val p = new Path(path)
    val fc = FileContext.getFileContext(p.toUri, conf)
    val files = fc.util.globStatus(p, HiddenFileFilter)

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
          val children = fc.listLocatedStatus(file.getPath)
          val iter = new Iterator[LocatedFileStatus] {
            override def hasNext: Boolean = children.hasNext
            override def next(): LocatedFileStatus = children.next
          }
          remaining ++= iter.filter(f => HiddenFileFilter.accept(f.getPath))
        }
      } else {
        PathUtils.getUncompressedExtension(file.getPath.getName).toLowerCase(Locale.US) match {
          case TAR       => result += new HadoopTarHandle(fc, file.getPath)
          case ZIP | JAR => result += new HadoopZipHandle(fc, file.getPath)
          case _         => result += new HadoopFileHandle(fc, file.getPath)
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

  class HadoopFileHandle(fc: FileContext, file: Path) extends FileHandle {

    override def path: String = file.toString

    override def exists: Boolean = fc.util.exists(file)

    override def length: Long = if (exists) { fc.getFileStatus(file).getLen } else { 0L }

    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val is = PathUtils.handleCompression(fc.open(file), file.getName)
      CloseableIterator.single(None -> is, is.close())
    }

    override def write(mode: CreateMode, createParents: Boolean): OutputStream = {
      mode.validate()
      val flags = java.util.EnumSet.noneOf(classOf[CreateFlag])
      if (mode.append) {
        flags.add(CreateFlag.APPEND)
      } else if (mode.overwrite) {
        flags.add(CreateFlag.OVERWRITE)
      }
      if (mode.create) {
        flags.add(CreateFlag.CREATE)
      }
      val ops = if (createParents) { CreateOpts.createParent() } else { CreateOpts.donotCreateParent() }
      fc.create(file, flags, ops) // TODO do we need to hsync/hflush?
    }

    override def delete(recursive: Boolean): Unit = {
      if (!fc.delete(file, recursive)) {
        throw new IOException(s"Could not delete file: $path")
      }
    }
  }

  class HadoopZipHandle(fc: FileContext, file: Path) extends HadoopFileHandle(fc, file) {
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      // we have to read the bytes into memory to get random access reads
      val bytes = WithClose(PathUtils.handleCompression(fc.open(file), file.getName)) { is =>
        IOUtils.toByteArray(is)
      }
      new ZipFileIterator(new ZipFile(new SeekableInMemoryByteChannel(bytes)), file.toString)
    }

    override def write(mode: CreateMode, createParents: Boolean): OutputStream =
      factory.createArchiveOutputStream(ArchiveStreamFactory.ZIP, super.write(mode, createParents))
  }

  class HadoopTarHandle(fc: FileContext, file: Path) extends HadoopFileHandle(fc, file) {
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val uncompressed = PathUtils.handleCompression(fc.open(file), file.getName)
      val archive = factory.createArchiveInputStream(ArchiveStreamFactory.TAR, uncompressed)
      new ArchiveFileIterator(archive, file.toString)
    }

    override def write(mode: CreateMode, createParents: Boolean): OutputStream =
      factory.createArchiveOutputStream(ArchiveStreamFactory.TAR, super.write(mode, createParents))
  }
}
