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
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
=======
=======
package org.locationtech.geomesa.utils.io.fs

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
import org.apache.commons.compress.archivers.ArchiveStreamFactory
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Options.CreateOpts
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/HadoopDelegate.scala
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
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
      val archive: ArchiveInputStream[_ <: ArchiveEntry] =
        factory.createArchiveInputStream(ArchiveStreamFactory.TAR, uncompressed)
      new ArchiveFileIterator(archive, file.toString)
    }

    override def write(mode: CreateMode, createParents: Boolean): OutputStream =
      factory.createArchiveOutputStream(ArchiveStreamFactory.TAR, super.write(mode, createParents))
  }
}
