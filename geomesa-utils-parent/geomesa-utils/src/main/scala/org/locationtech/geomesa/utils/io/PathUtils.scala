/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.{FileSystemDelegate, LocalDelegate}

import java.io._
<<<<<<< HEAD:geomesa-utils-parent/geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/PathUtils.scala
import java.net.URL
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
=======
import java.net.{MalformedURLException, URL}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicBoolean
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/PathUtils.scala
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/PathUtils.scala
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/io/PathUtils.scala
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
import java.util.regex.Pattern
import scala.util.Try

object PathUtils extends FileSystemDelegate with LazyLogging {

  private val uriRegex = Pattern.compile("""\w+://.*""")

  private val localDelegate = new LocalDelegate()

  // delegate allows us to avoid a runtime dependency on hadoop
  private val hadoopDelegate: FileSystemDelegate =
    Try(Class.forName("org.locationtech.geomesa.utils.hadoop.HadoopDelegate").newInstance())
        .getOrElse(null)
        .asInstanceOf[FileSystemDelegate]

  override def interpretPath(path: String): Seq[FileHandle] = chooseDelegate(path).interpretPath(path)

  override def getHandle(path: String): FileHandle = chooseDelegate(path).getHandle(path)

  override def getUrl(path: String): URL = chooseDelegate(path).getUrl(path)

  /**
    * Checks to see if the path uses a URL pattern and then if it is *not* file://
    *
    * @param path Input resource path
    * @return     Whether or not the resource is remote.
    */
  def isRemote(path: String): Boolean =
    uriRegex.matcher(path).matches() && !path.toLowerCase.startsWith("file://")

  /**
    * Returns the file extension, minus any compression that may be present
    *
    * @param path file path
    * @return
    */
  def getUncompressedExtension(path: String): String =
    FilenameUtils.getExtension(CompressionUtils.getUncompressedFilename(path))

  /**
    * Gets the base file name and the extension. Useful for adding unique ids to a common file name,
    * while preserving the extension
    *
    * @param path path
    * @param includeDot if true, the '.' will be preserved in the extension, otherwise it will be dropped
    * @return (base name including path prefix, extension)
    */
  def getBaseNameAndExtension(path: String, includeDot: Boolean = true): (String, String) = {
    def dotIndex(base: Int): Int = if (includeDot) { base } else { base + 1}
    val split = FilenameUtils.indexOfExtension(path)
    if (split == -1) { (path, "") } else {
      val withoutExtension = path.substring(0, split)
      // look for file names like 'foo.tar.gz'
      val secondSplit = FilenameUtils.indexOfExtension(withoutExtension)
      if (secondSplit != -1 && CompressionUtils.isCompressedFilename(path)) {
        (path.substring(0, secondSplit), path.substring(dotIndex(secondSplit)))
      } else {
        (withoutExtension, path.substring(dotIndex(split)))
      }
    }
  }

  /**
    * Wrap the input stream in a decompressor, if the file is compressed
    *
    * @param is input stream
    * @param filename filename (used to determine compression)
    * @return
    */
  def handleCompression(is: InputStream, filename: String): InputStream = {
    val buffered = new BufferedInputStream(is)
    CompressionUtils.Utils.find(_.isCompressedFilename(filename)) match {
      case None => buffered
      case Some(utils) => utils.compress(buffered)
    }
  }

  /**
    * Delete a path, including all children
    *
    * @param path path
    */
  def deleteRecursively(path: Path): Unit = Files.walkFileTree(path, new DeleteFileVisitor)

  private def chooseDelegate(path: String): FileSystemDelegate =
    if (hadoopDelegate != null && uriRegex.matcher(path).matches()) { hadoopDelegate } else { localDelegate }

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
