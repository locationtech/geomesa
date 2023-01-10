/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.io.input.CloseShieldInputStream
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.{CreateMode, FileHandle}
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.{LocalFileHandle, LocalTarHandle, LocalZipHandle}
import org.locationtech.geomesa.utils.io.{CopyingInputStream, PathUtils, WithClose}

import java.io._
<<<<<<< HEAD:geomesa-utils-parent/geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/LocalDelegate.scala
import java.net.URL
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/fs/LocalDelegate.scala
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.Locale
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class LocalDelegate extends FileSystemDelegate with LazyLogging {

  import ArchiveStreamFactory.{JAR, TAR, ZIP}

  import scala.collection.JavaConverters._

  override def getHandle(path: String): FileHandle = createHandle(new File(path))

  override def interpretPath(path: String): Seq[FileHandle] = {
    val firstWildcard = path.indexOf('*')
    if (firstWildcard == -1) {
      val file = new File(path)
      if (file.isDirectory) {
        logger.warn(s"Input file is a directory: ${file.getAbsolutePath}")
        Seq.empty
      } else {
        Seq(createHandle(file))
      }
    } else {
      // find the base directory to search from based on the non-wildcard prefix
      val lastSep = path.length - 1 - path.reverse.indexOf('/', path.length - firstWildcard - 1)
      val (basepath, glob) = if (lastSep == path.length) {
        (new File(".").toPath, path)
      } else {
        (new File(path.substring(0, lastSep)).toPath, path.substring(lastSep + 1))
      }
      if (glob.indexOf('/') == -1 && !glob.contains("**")) {
        // we can just look in the current directory
        WithClose(Files.newDirectoryStream(basepath, glob)) { stream =>
          stream.asScala.toList.flatMap{ p =>
            val file = p.toFile
            if (file.isDirectory) { Nil } else { List(createHandle(file)) }
          }
        }
      } else {
        // we have to walk the file tree
        val matcher = FileSystems.getDefault.getPathMatcher("glob:" + glob)
        val result = ArrayBuffer.empty[FileHandle]
        val visitor = new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
            if (matcher.matches(file) && attributes.isRegularFile && !attributes.isDirectory) {
              result += createHandle(file.toFile)
            }
            FileVisitResult.CONTINUE
          }
        }
        Files.walkFileTree(basepath, visitor)
        result.toSeq
      }
    }
  }

  override def getUrl(path: String): URL = new File(path).toURI.toURL

  private def createHandle(file: File): FileHandle = {
    PathUtils.getUncompressedExtension(file.getName).toLowerCase(Locale.US) match {
      case TAR       => new LocalTarHandle(file)
      case ZIP | JAR => new LocalZipHandle(file)
      case _         => new LocalFileHandle(file)
    }
  }
}

object LocalDelegate {

  private val factory = new ArchiveStreamFactory()

  class LocalFileHandle(file: File) extends FileHandle {

    override def path: String = file.getAbsolutePath

    override def exists: Boolean = file.exists()

    override def length: Long = file.length()

    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val is = PathUtils.handleCompression(new FileInputStream(file), file.getName)
      CloseableIterator.single(None -> is, is.close())
    }

    override def write(mode: CreateMode, createParents: Boolean): OutputStream = {
      mode.validate()
      if (file.exists()) {
        if (mode.append) {
          new FileOutputStream(file, true)
        } else if (mode.overwrite) {
          new FileOutputStream(file, false)
        } else {
          // due to validation, must be mode.create
          throw new FileAlreadyExistsException(s"File already exists for mode 'create': $path")
        }
      } else if (!mode.create) {
        throw new FileNotFoundException(s"File does not exist: $path")
      } else {
        val parent = file.getParentFile
        if (parent != null && !parent.exists()) {
          if (!createParents) {
            throw new FileNotFoundException(s"Parent file does not exist: $path")
          } else if (!parent.mkdirs()) {
            throw new IOException(s"Parent file does not exist and could not be created: $path")
          }
        }
        new FileOutputStream(file)
      }
    }

    override def delete(recursive: Boolean): Unit = {
      if (recursive) {
        PathUtils.deleteRecursively(file.toPath)
      } else if (!file.delete()) {
        throw new IOException(s"Could not delete file: $path")
      }
    }
  }

  class LocalZipHandle(file: File) extends LocalFileHandle(file) {
    override def open: CloseableIterator[(Option[String], InputStream)] =
      new ZipFileIterator(new ZipFile(file), file.getAbsolutePath)
    override def write(mode: CreateMode, createParents: Boolean): OutputStream =
      factory.createArchiveOutputStream(ArchiveStreamFactory.ZIP, super.write(mode, createParents))
  }

  class LocalTarHandle(file: File) extends LocalFileHandle(file) {
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val uncompressed = PathUtils.handleCompression(new FileInputStream(file), file.getName)
      val archive: ArchiveInputStream[_ <: ArchiveEntry] =
        factory.createArchiveInputStream(ArchiveStreamFactory.TAR, uncompressed)
      new ArchiveFileIterator(archive, file.getAbsolutePath)
    }
    override def write(mode: CreateMode, createParents: Boolean): OutputStream =
      factory.createArchiveOutputStream(ArchiveStreamFactory.TAR, super.write(mode, createParents))
  }

  private class StdInHandle(in: InputStream) extends FileHandle {
    override def path: String = "<stdin>"
    override def exists: Boolean = false
    override def length: Long = Try(in.available().toLong).getOrElse(0L) // .available will throw if stream is closed
    override def open: CloseableIterator[(Option[String], InputStream)] =
      CloseableIterator.single(None -> CloseShieldInputStream.wrap(in))
    override def write(mode: CreateMode, createParents: Boolean): OutputStream = System.out
    override def delete(recursive: Boolean): Unit = {}
  }

  object StdInHandle {
    // hook to allow for unit testing stdin
    val SystemIns: ThreadLocal[InputStream] = new ThreadLocal[InputStream]() {
      override def initialValue(): InputStream = System.in
    }
    def get(): FileHandle = new StdInHandle(SystemIns.get)
    // avoid hanging if there isn't any input
    def available(): Option[FileHandle] = if (isAvailable) { Some(get()) } else { None }
    def isAvailable: Boolean = SystemIns.get.available() > 0
  }

  // A class that caches any bytes read from the stdin input stream
  private class CachingStdInHandle(in: InputStream) extends StdInHandle(in) {
    private val is = new CopyingInputStream(in, 1024)
    private var cachedBytes: Array[Byte] = Array.empty

    override def length: Long = super.length + cachedBytes.length

    override def open: CloseableIterator[(Option[String], InputStream)] =
      CloseableIterator.single(None -> new CopyOnClose())

    private class CopyOnClose
        extends SequenceInputStream(new ByteArrayInputStream(cachedBytes), CloseShieldInputStream.wrap(is)) {
      override def close(): Unit = {
        try { super.close() } finally {
          if (is.copied > 0) {
            cachedBytes ++= is.replay(is.copied)
          }
        }
      }
    }
  }

  object CachingStdInHandle {
    def get(): FileHandle = new CachingStdInHandle(StdInHandle.SystemIns.get)
    // avoid hanging if there isn't any input
    def available(): Option[FileHandle] = if (isAvailable) { Some(get()) } else { None }
    def isAvailable: Boolean = StdInHandle.isAvailable
  }
}
