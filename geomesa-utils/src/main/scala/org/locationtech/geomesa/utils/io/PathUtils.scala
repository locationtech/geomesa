/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io._
import java.net.{MalformedURLException, URL}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2Utils}
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.{XZCompressorInputStream, XZUtils}
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.{FileSystemDelegate, HadoopDelegate, LocalDelegate}

import scala.util.Try

object PathUtils extends FileSystemDelegate with LazyLogging {

  private val uriRegex = Pattern.compile("""\w+://.*""")

  private val hadoopAvailable = Try(Class.forName("org.apache.hadoop.conf.Configuration")).isSuccess

  private val localDelegate = new LocalDelegate

  // delegate allows us to avoid a runtime dependency on hadoop
  private val hadoopDelegate = if (hadoopAvailable) { new HadoopDelegate } else { null }

  private val factorySet = new AtomicBoolean(false)

  // Make sure that the Hadoop URL Factory is configured.
  def configureURLFactory(): Unit = {
    if (factorySet.compareAndSet(false, true)) {
      try { // Calling this method twice in the same JVM causes a java.lang.Error
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory)
        logger.trace("Configured Hadoop URL Factory.")
      } catch {
        case _: Throwable =>
          logger.warn("Could not register Hadoop URL Factory.  Some filesystems may not be available.")
      }
    }
  }

  /**
    * Checks to see if the path uses a URL pattern and then if it is *not* file://
    * @param path Input resource path
    * @return     Whether or not the resource is remote.
    */
  def isRemote(path: String): Boolean =
    uriRegex.matcher(path).matches() && !path.toLowerCase.startsWith("file://")

  /**
    * Registers Hadoop URL handlers via #configureURLFactory()
    *
    * @param path Input URL string
    * @return     URL instance
    */
  def getUrl(path: String): URL = {
    try {
      if (isRemote(path)) {
        // we need to add the hadoop url factories to the JVM to support hdfs, S3, or wasb
        // we only want to call this once per jvm or it will throw an error
        configureURLFactory()
        new URL(path)
      } else {
        new File(path).toURI.toURL
      }
    }
    catch {
      case e: MalformedURLException => throw new IllegalArgumentException(s"Invalid URL $path: ", e)
    }
  }

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
