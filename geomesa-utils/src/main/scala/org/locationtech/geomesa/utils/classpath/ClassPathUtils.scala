/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.classpath

import java.io.{File, FileFilter, FilenameFilter}
import java.net.{URLClassLoader, URLDecoder}

import com.typesafe.scalalogging.slf4j.Logging

import scala.collection.mutable.ArrayBuffer

object ClassPathUtils extends Logging {

  private val jarFileFilter = new FilenameFilter() {
    override def accept(dir: File, name: String) =
      name.endsWith(".jar") && !name.endsWith("-sources.jar") && !name.endsWith("-javadoc.jar")
  }

  private val folderFileFilter = new FileFilter() {
    override def accept(pathname: File) = pathname.isDirectory
  }

  def findJars(jars: Seq[String], searchPath: Iterator[() => Seq[File]]): Seq[File] = {
    val foundJars = ArrayBuffer.empty[File]
    var remaining = jars
    // search each path in order until we've found all our jars
    while (remaining.nonEmpty && searchPath.hasNext) {
      val files = searchPath.next()()
      remaining = remaining.filter { jarPrefix =>
        val matched = files.filter(_.getName.startsWith(jarPrefix))
        foundJars ++= matched
        matched.isEmpty
      }
    }

    if (remaining.nonEmpty) {
      logger.warn(s"Could not find requested jars: $remaining")
    }

    foundJars.distinct.toSeq
  }

  /**
   * Finds URLs of jar files based on an environment variable
   *
   * @param home
   * @return
   */
  def getJarsFromEnvironment(home: String): Seq[File] =
    sys.env.get(home).map(new File(_)).filter(_.isDirectory).toSeq.flatMap(loadJarsFromFolder)

  /**
   * Finds URLs of jar files based on the current classpath
   *
   * @param clas
   * @return
   */
  def getJarsFromClasspath(clas: Class[_]): Seq[File] = {
    clas.getClassLoader match {
      case cl: URLClassLoader => cl.getURLs.map(u => new File(cleanClassPathURL(u.getFile)))
      case cl =>
        logger.warn(s"Can't load jars from classloader of type ${cl.getClass.getCanonicalName}")
        Seq.empty
    }
  }

  // noinspection AccessorLikeMethodIsEmptyParen
  def getJarsFromSystemClasspath(): Seq[File] = {
    val urls = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader].getURLs
    urls.map(u => new File(cleanClassPathURL(u.getFile)))
  }

  /**
   * Recursively searches folders for jar files
   *
   * @param dir
   * @return
   */
  def loadJarsFromFolder(dir: File): Seq[File] = {
    val files = Option(dir.listFiles(jarFileFilter)).toSeq.flatten
    val children = Option(dir.listFiles(folderFileFilter)).toSeq.flatten.flatMap(loadJarsFromFolder)
    files ++ children
  }

  def cleanClassPathURL(url: String): String =
    URLDecoder.decode(url, "UTF-8").replace("file:", "").replace("!", "")

}
