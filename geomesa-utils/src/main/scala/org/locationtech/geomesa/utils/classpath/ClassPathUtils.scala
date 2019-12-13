/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.classpath

import java.io.{File, FileFilter, FilenameFilter}
import java.net.{URLClassLoader, URLDecoder}

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer

object ClassPathUtils extends LazyLogging {

  private val jarFileFilter = new FilenameFilter() {
    override def accept(dir: File, name: String) =
      name.endsWith(".jar") && !name.endsWith("-sources.jar") && !name.endsWith("-javadoc.jar")
  }

  private val folderFileFilter = new FileFilter() {
    override def accept(pathname: File) = pathname.isDirectory
  }

  private val fileFilter = new FileFilter() {
    override def accept(pathname: File) = pathname.isFile()
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
    * Finds URLs of jar files based on an environment variable
    *
    * @param home
    * @param path - the path to append to the result of the env var
    * @return
    */
  def getJarsFromEnvironment(home: String, path: String): Seq[File] =
    sys.env.get(home).map(h => new File(new File(h), path)).filter(_.isDirectory).toSeq.flatMap(loadJarsFromFolder)

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

  /**
    * Finds URLs of files based on a system property
    *
    * @param prop
    * @return
    */
  def getFilesFromSystemProperty(prop: String): Seq[File] = {
    Option(System.getProperty(prop)) match {
      case Some(path) => path.toString().split(":").map(new File(_)).toSeq.flatMap(loadFiles)
      case None =>
        logger.debug(s"No files loaded onto classpath from system property: ${prop}")
        Seq.empty
    }
  }

  /**
    * Recursively searches file for all files. Accepts file or dir.
    *
    * @param file
    * @return
    */
  def loadFiles(file: File): Seq[File] = {
    if (file.isDirectory) {
      val files = Option(file.listFiles(fileFilter)).toSeq.flatten
      val childDirs = Option(file.listFiles(folderFileFilter)).toSeq.flatten.flatMap(loadFiles)
      files ++ childDirs
    } else {
      Option(file).toSeq
    }
  }

  def cleanClassPathURL(url: String): String =
    URLDecoder.decode(url, "UTF-8").replace("file:", "").replace("!", "")

  /**
    * <p>Load files (jars, resources, configuration, etc) from a classpath defined by an environmental
    * variable following these rules:
    * <ul>
    *   <li>Entries are colon (:) separated</li>
    *   <li>If the entry ends with "&#47;*", treat it as a directory, and list jars in that
    *   directory...no recursion</li>
    *   <li>If the entry is a file then add it</li>
    *   <li>If the entry is a directory list all files (jars and files) in the directory</li>
    * </ul>
    * </p>
    *
    * @param prop - environmental variable
    * @return a list of files found in the classpath
    */
  def loadClassPathFromEnv(prop: String): Seq[File] = {
    val files = sys.env.get(prop).toSeq.flatMap(_.split(':').toSeq).flatMap { entry =>
      if (entry.endsWith("/*")) {
        new File(entry.dropRight(2)).listFiles(jarFileFilter)
      } else {
        val f = new File(entry)
        if (f.isDirectory) {
          Option(f.listFiles).toSeq.flatten
        } else {
          Seq(f)
        }
      }
    }
    logger.debug(s"Loaded env classpath '$prop': ${files.map(_.getAbsolutePath).mkString(":")}")
    files
  }
}
