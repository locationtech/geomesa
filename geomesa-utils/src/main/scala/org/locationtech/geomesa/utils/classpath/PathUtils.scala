/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.classpath

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.zip.GZIPInputStream

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream, BZip2Utils}
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.{XZCompressorInputStream, XZCompressorOutputStream, XZUtils}

object PathUtils {

  def interpretPath(path: String): List[File] = {
    val firstWildcard = path.indexOf('*')
    if (firstWildcard == -1) {
      List(new File(path))
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
        val stream = Files.newDirectoryStream(basepath, glob)
        try { stream.map(_.toFile).toList } finally { stream.close() }
      } else {
        // we have to walk the file tree
        val matcher = FileSystems.getDefault.getPathMatcher("glob:" + glob)
        val result = ArrayBuffer.empty[File]
        val visitor = new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
            if (matcher.matches(file) && attributes.isRegularFile && !attributes.isDirectory) {
              result.append(file.toFile)
            }
            FileVisitResult.CONTINUE
          }
        }
        Files.walkFileTree(basepath, visitor)
        result.toList
      }
    }
  }

  def getInputStream(f: File): InputStream = {
    val path = f.getPath
      path match {
        case _ if GzipUtils.isCompressedFilename(path)  => new GZIPInputStream(new BufferedInputStream(new FileInputStream(f)))
        case _ if BZip2Utils.isCompressedFilename(path) => new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(f)))
        case _ if XZUtils.isCompressedFilename(path)    => new XZCompressorInputStream(new BufferedInputStream(new FileInputStream(f)))
        case _ => new BufferedInputStream(new FileInputStream(f))
      }
  }

  def getSource(f: File): BufferedSource = {
    val name = f.getName.toLowerCase
    if (name.endsWith(".gz")) {
      Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(f))), "UTF-8")
    } else {
      Source.fromFile(f, "UTF-8")
    }
  }
}
