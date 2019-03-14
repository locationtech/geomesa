/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import java.io._
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.Locale

import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.zip.ZipFile
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.{LocalFileHandle, LocalTarHandle, LocalZipHandle}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class LocalDelegate extends FileSystemDelegate {

  import scala.collection.JavaConverters._

  override def interpretPath(path: String): Seq[FileHandle] = {
    val firstWildcard = path.indexOf('*')
    if (firstWildcard == -1) {
      val file = new File(path)
      if (file.isDirectory) {
        throw new IllegalArgumentException(s"Input file is a directory: ${file.getAbsolutePath}")
      }
      Seq(createHandle(file))
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
        result
      }
    }
  }

  private def createHandle(file: File): FileHandle = {
    import ArchiveStreamFactory.{JAR, TAR, ZIP}
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
    override def length: Long = file.length()
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val is = PathUtils.handleCompression(new FileInputStream(file), file.getName)
      CloseableIterator.single(None -> is, is.close())
    }
  }

  class LocalZipHandle(file: File) extends LocalFileHandle(file) {
    override def open: CloseableIterator[(Option[String], InputStream)] =
      new ZipFileIterator(new ZipFile(file), file.getAbsolutePath)
  }

  class LocalTarHandle(file: File) extends LocalFileHandle(file) {
    override def open: CloseableIterator[(Option[String], InputStream)] = {
      val uncompressed = PathUtils.handleCompression(new FileInputStream(file), file.getName)
      val archive = factory.createArchiveInputStream(ArchiveStreamFactory.TAR, uncompressed)
      new ArchiveFileIterator(archive, file.getAbsolutePath)
    }
  }

  class StdInHandle extends FileHandle {
    override def path: String = "<stdin>"
    override def length: Long = Try(System.in.available().toLong).getOrElse(0L) // .available will throw if stream is closed
    override def open: CloseableIterator[(Option[String], InputStream)] = CloseableIterator.single(None -> System.in)
  }

  object StdInHandle {
    // avoid hanging if there isn't any input
    def available(): Option[FileHandle] = if (isAvailable) { Some(new StdInHandle) } else { None }
    def isAvailable: Boolean = System.in.available() > 0
  }
}
