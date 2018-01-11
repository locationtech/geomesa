/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.Locale
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2Utils}
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.{XZCompressorInputStream, XZUtils}
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.{FileSystemDelegate, HadoopDelegate, LocalDelegate}

import scala.io.Source
import scala.util.Try

object PathUtils extends FileSystemDelegate {

  private val uriRegex = Pattern.compile("""\w+://.*""")

  private val hadoopAvailable = Try(Class.forName("org.apache.hadoop.conf.Configuration")).isSuccess

  private val localDelegate = new LocalDelegate

  // delegate allows us to avoid a runtime dependency on hadoop
  private val hadoopDelegate = if (hadoopAvailable) { new HadoopDelegate } else { null }

  val RemotePrefixes: Seq[String] = {
    val is = getClass.getResourceAsStream("remote-prefixes.list")
    WithClose(Source.fromInputStream(is, StandardCharsets.UTF_8.displayName)) { source =>
      source.getLines.map(s => s"${s.toLowerCase(Locale.US)}://").toList
    }
  }

  def isRemote(path: String): Boolean = RemotePrefixes.exists(path.toLowerCase(Locale.US).startsWith)

  override def interpretPath(path: String): Seq[FileHandle] = chooseDelegate(path).interpretPath(path)

  def handleCompression(is: InputStream, filename: String): InputStream = {
    if (GzipUtils.isCompressedFilename(filename)) {
      new GZIPInputStream(new BufferedInputStream(is))
    } else if (BZip2Utils.isCompressedFilename(filename)) {
      new BZip2CompressorInputStream(new BufferedInputStream(is))
    } else if (XZUtils.isCompressedFilename(filename)) {
      new XZCompressorInputStream(new BufferedInputStream(is))
    } else {
      new BufferedInputStream(is)
    }
  }

  def deleteRecursively(f: Path): Unit = Files.walkFileTree(f, new DeleteFileVisitor)

  private def chooseDelegate(path: String): FileSystemDelegate =
    if (hadoopAvailable && uriRegex.matcher(path).matches()) { hadoopDelegate } else { localDelegate }

  /**
    * File visitor to delete nested paths
    */
  class DeleteFileVisitor extends FileVisitor[Path] {

    override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE

    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if (!attrs.isDirectory) {
        Files.delete(file)
      }
      FileVisitResult.CONTINUE
    }

    override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = FileVisitResult.CONTINUE

    override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
      Files.delete(dir)
      FileVisitResult.CONTINUE
    }
  }
}
