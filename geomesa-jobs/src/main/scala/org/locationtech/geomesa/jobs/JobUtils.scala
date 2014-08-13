/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs

import java.io.{File, FileFilter, FilenameFilter}
import java.net.{URLClassLoader, URLDecoder}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.Connector
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.core.data.AccumuloDataStore

object JobUtils extends Logging {

  // default jars that will be included with m/r jobs
  val defaultLibJars = Seq("geomesa",
                           "accumulo-core",
                           "accumulo-fate",
                           "accumulo-start",
                           "accumulo-trace",
                           "gt-referencing",
                           "gt-grid",
                           "gt-transform",
                           "gt-main",
                           "gt-api",
                           "gt-data",
                           "gt-opengis",
                           "gt-cql",
                           "gt-metadata",
                           "gt-render",
                           "gt-coverage",
                           "gt-process-feature",
                           "gt-process",
                           "gt-shapefile",
                           "libthrift",
                           "jts",
                           "jsr-275",
                           "hsqldb",
                           "commons-pool")

  /**
   * Sets the libjars into a Hadoop configuration. Will search the environment first, then the
   * classpath, until all required jars have been found.
   *
   * @param conf
   * @param libJars
   */
  def setLibJars(conf: Configuration, libJars: Seq[String] = defaultLibJars): Unit = {
    val paths = Iterator(getUrlsFromEnvironment("GEOMESA_HOME"),
                         getUrlsFromEnvironment("ACCUMULO_HOME"),
                         getUrlsFromClasspath(classOf[AccumuloDataStore]),
                         getUrlsFromClasspath(classOf[Connector]))

    val libjars = scala.collection.mutable.Set.empty[String]
    var remaining = libJars
    // search each path in order until we've found all our jars
    while (!remaining.isEmpty && paths.hasNext) {
      val urls = paths.next()
      remaining = remaining.filter { jarPrefix =>
        val matched = urls.filter(url => url.getName.startsWith(jarPrefix))
        libjars ++= matched.map(url => "file:///" + url.getAbsolutePath)
        matched.isEmpty
      }
    }

    if (!remaining.isEmpty) {
      logger.warn(s"Could not find required jars: $remaining")
    }

    // tmpjars is the hadoop config that corresponds to libjars
    conf.setStrings("tmpjars", libjars.toSeq: _*)

    logger.trace(s"libjars=${libjars.mkString("\n", "\n", "")}")
  }

  /**
   * Finds URLs of jar files based on an environment variable
   *
   * @param home
   * @return
   */
  def getUrlsFromEnvironment(home: String): Seq[File] =
    sys.env.get(home)
      .map(f => new File(new File(f), "lib"))
      .filter(_.isDirectory)
      .map(loadJarsFromFolder)
      .getOrElse(Seq.empty)

  /**
   * Finds URLs of jar files based on the current classpath
   *
   * @param clas
   * @return
   */
  def getUrlsFromClasspath(clas: Class[_]): Seq[File] = {
    val urls =
      clas.getClassLoader match {
        case cl: VFSClassLoader =>
          cl.getFileObjects.map(u => u.getURL)
        case cl: URLClassLoader =>
          cl.getURLs
      }
    urls.map(u => new File(cleanClassPathURL(u.getFile)))
  }

  /**
   * Recursively searches folders for jar files
   *
   * @param dir
   * @return
   */
  def loadJarsFromFolder(dir: File): Seq[File] = {
    val files = dir.listFiles(new FilenameFilter() {
      override def accept(dir: File, name: String) =
        name.endsWith(".jar") && !name.endsWith("-sources.jar") && !name.endsWith("-javadoc.jar")
    })
    files ++ dir.listFiles(new FileFilter() {
      override def accept(pathname: File) = pathname.isDirectory
    }).flatMap(f => loadJarsFromFolder(f))
  }

  def cleanClassPathURL(url: String): String =
    URLDecoder.decode(url, "UTF-8").replace("file:", "").replace("!", "")
}
