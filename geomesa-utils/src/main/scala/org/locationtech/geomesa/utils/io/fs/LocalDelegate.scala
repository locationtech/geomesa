/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import java.io.{File, FileInputStream, InputStream}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicBoolean

import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.LocalFileHandle
import org.locationtech.geomesa.utils.stats.CountingInputStream

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class LocalDelegate extends FileSystemDelegate {

  override def interpretPath(path: String): Seq[FileHandle] = {

    val firstWildcard = path.indexOf('*')
    if (firstWildcard == -1) {
      Seq(new LocalFileHandle(new File(path)))
    } else {
      // find the base directory to search from based on the non-wildcard prefix
      val lastSep = path.length - 1 - path.reverse.indexOf('/', path.length - firstWildcard - 1)
      val (basepath, glob) = if (lastSep == path.length) {
        (new File(".").toPath, path)
      } else {
        (new File(path.substring(0, lastSep)).toPath, path.substring(lastSep + 1))
      }
      if (glob.indexOf('/') == -1 && !glob.contains("**")) {
        import scala.collection.JavaConversions._
        // we can just look in the current directory
        val stream = Files.newDirectoryStream(basepath, glob)
        try { stream.map(s => new LocalFileHandle(s.toFile)).toSeq } finally { stream.close() }
      } else {
        // we have to walk the file tree
        val matcher = FileSystems.getDefault.getPathMatcher("glob:" + glob)
        val result = ArrayBuffer.empty[FileHandle]
        val visitor = new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
            if (matcher.matches(file) && attributes.isRegularFile && !attributes.isDirectory) {
              result.append(new LocalFileHandle(file.toFile))
            }
            FileVisitResult.CONTINUE
          }
        }
        Files.walkFileTree(basepath, visitor)
        result
      }
    }
  }
}

object LocalDelegate {

  class LocalFileHandle(file: File) extends FileHandle {
    override def path: String = file.getAbsolutePath
    override def length: Long = file.length()
    override def open: InputStream = new FileInputStream(file)
  }

  class StdInHandle extends FileHandle {
    override def path: String = "<stdin>"
    override def length: Long = Try(System.in.available().toLong).getOrElse(0L) // .available will throw if stream is closed
    override def open: InputStream = System.in
  }

  object StdInHandle {
    // avoid hanging if there isn't any input
    def available(): Option[FileHandle] = if (isAvailable) { Some(new StdInHandle) } else { None }

    def isAvailable: Boolean = System.in.available() > 0
  }
}
