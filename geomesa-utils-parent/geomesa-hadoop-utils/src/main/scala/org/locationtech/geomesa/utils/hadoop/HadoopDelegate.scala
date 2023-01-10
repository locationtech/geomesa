/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

<<<<<<< HEAD:geomesa-utils-parent/geomesa-hadoop-utils/src/main/scala/org/locationtech/geomesa/utils/hadoop/HadoopDelegate.scala
package org.locationtech.geomesa.utils.hadoop

import com.typesafe.scalalogging.LazyLogging
<<<<<<< HEAD
=======
<<<<<<< HEAD
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
=======
=======
package org.locationtech.geomesa.utils.io.fs

>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
import org.apache.commons.compress.archivers.ArchiveStreamFactory
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c55f214e5cd (Merge branch 'a0x8o' into stag0)
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.hadoop.HadoopDelegate.{HadoopFileHandle, HadoopTarHandle, HadoopZipHandle}
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.{CreateMode, FileHandle}
import org.locationtech.geomesa.utils.io.fs.{ArchiveFileIterator, FileSystemDelegate, ZipFileIterator}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import java.io.{IOException, InputStream, OutputStream}
<<<<<<< HEAD:geomesa-utils-parent/geomesa-hadoop-utils/src/main/scala/org/locationtech/geomesa/utils/hadoop/HadoopDelegate.scala
import java.net.{MalformedURLException, URL}
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
import java.util.Locale
import scala.collection.mutable.ListBuffer

/**
  * Delegate allows us to avoid a runtime dependency on hadoop
  */
class HadoopDelegate(conf: Configuration) extends FileSystemDelegate {

  import ArchiveStreamFactory.{JAR, TAR, ZIP}
  import HadoopDelegate.HiddenFileFilter
  import org.apache.hadoop.fs.{LocatedFileStatus, Path}

  def this() = this(new Configuration())

  // use the same property as FileInputFormat
  private val recursive = conf.getBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false)

  // we need to add the hadoop url factories to the JVM to support hdfs, S3, or wasb
  // we only want to call this once per jvm or it will throw an error
  HadoopDelegate.configureURLFactory()

  override def getHandle(path: String): FileHandle = {
    val p = new Path(path)
    val fs = FileSystem.get(p.toUri, conf)
    PathUtils.getUncompressedExtension(p.getName).toLowerCase(Locale.US) match {
      case TAR       => new HadoopTarHandle(fs, p)
      case ZIP | JAR => new HadoopZipHandle(fs, p)
      case _         => new HadoopFileHandle(fs, p)
    }
  }

  // based on logic from hadoop FileInputFormat
  override def interpretPath(path: String): Seq[FileHandle] = {
    val p = new Path(path)
    val fs = FileSystem.get(p.toUri, conf)
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
          case TAR       => result += new HadoopTarHandle(fs, file.getPath)
          case ZIP | JAR => result += new HadoopZipHandle(fs, file.getPath)
          case _         => result += new HadoopFileHandle(fs, file.getPath)
        }
      }
    }

    result.result
  }

  override def getUrl(path: String): URL = {
    try { new URL(path) } catch {
      case e: MalformedURLException => throw new IllegalArgumentException(s"Invalid URL $path: ", e)
    }
  }
}

object HadoopDelegate extends LazyLogging {

  private val factory = new ArchiveStreamFactory()

  private var setUrlFactory = true

  val HiddenFileFilter: PathFilter = new PathFilter() {
    override def accept(path: Path): Boolean = {
      val name = path.getName
      !name.startsWith("_") && !name.startsWith(".")
    }
  }

  /**
   * Ensure that the Hadoop URL Factory is configured, so that urls staring with hdfs:// can be parsed
   */
  private def configureURLFactory(): Unit = synchronized {
    if (setUrlFactory) {
      setUrlFactory = false
      try { // Calling this method twice in the same JVM causes a java.lang.Error
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory)
        logger.trace("Configured Hadoop URL Factory")
      } catch {
        case _: Throwable =>
          logger.warn("Could not register Hadoop URL Factory. Some filesystems may not be available.")
      }
    }
  }

  class HadoopFileHandle(fs: FileSystem, file: Path) extends FileHandle {

    override def path: String = file.toString

    override def exists: Boolean = fs.exists(file)

    override def length: Long = if (exists) { fs.getFileStatus(file).getLen } else { 0L }

    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val is = PathUtils.handleCompression(fs.open(file), file.getName)
      CloseableIterator.single(None -> is, is.close())
    }

    override def write(mode: CreateMode): OutputStream = {
      mode.validate()
      if (mode.append) {
        fs.append(file)
      } else {
        fs.create(file, mode.overwrite) // TODO do we need to hsync/hflush?
      }
    }

    override def delete(recursive: Boolean): Unit = {
      if (!fs.delete(file, recursive)) {
        throw new IOException(s"Could not delete file: $path")
      }
    }
  }

  class HadoopZipHandle(fs: FileSystem, file: Path) extends HadoopFileHandle(fs, file) {
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      // we have to read the bytes into memory to get random access reads
      val bytes = WithClose(PathUtils.handleCompression(fs.open(file), file.getName)) { is =>
        IOUtils.toByteArray(is)
      }
      new ZipFileIterator(new ZipFile(new SeekableInMemoryByteChannel(bytes)), file.toString)
    }

    override def write(mode: CreateMode): OutputStream =
      factory.createArchiveOutputStream(ArchiveStreamFactory.ZIP, super.write(mode))
  }

  class HadoopTarHandle(fs: FileSystem, file: Path) extends HadoopFileHandle(fs, file) {
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val uncompressed = PathUtils.handleCompression(fs.open(file), file.getName)
      val archive: ArchiveInputStream[_ <: ArchiveEntry] =
        factory.createArchiveInputStream(ArchiveStreamFactory.TAR, uncompressed)
      new ArchiveFileIterator(archive, file.toString)
    }

    override def write(mode: CreateMode): OutputStream =
      factory.createArchiveOutputStream(ArchiveStreamFactory.TAR, super.write(mode))
  }
}
