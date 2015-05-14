/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.conf

import org.apache.accumulo.core.conf.{AccumuloConfiguration, DefaultConfiguration}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._

object AccGeoConfiguration {

  // utility method to load a configuration file that need not already be on
  // the Java class path
  def loadResource(filepath:String="geomesa.xml") = {
    if (filepath==null) throw new Exception("Invalid filepath")
    val file = new java.io.File(filepath)
    val urlLoader = AccGeoConfiguration.getClass.getClassLoader.
      asInstanceOf[java.net.URLClassLoader]
    urlLoader.getResource(file.getName) match {
      case null => {
        val dir = file.getParent
        if (dir!=null) {
          val dirURL = new java.io.File(dir).toURI.toURL
          val loader = new java.net.URLClassLoader(Array(dirURL), urlLoader)
          loader.getResource(file.getName)
        } else null
      }
      case a:AnyRef => a
    }
  }

  case class Property(name: String, defaultValue: String, description: String)
  val INSTANCE_GEO_DFS_DIR = Property(GEO_DFS_DIR, "/geomesa/lib", "Location of the GeoMesa lib dir")

  lazy val accConf: DefaultConfiguration = AccumuloConfiguration.getDefaultConfiguration

  lazy val accGeoConf = {
    val configFile = System.getProperty("geomesa.config.file", "geomesa-site.xml")
    val c = new Configuration(false)
    loadResource(configFile) match {
      case null => println("WARN: Config '" + configFile + "' not available")
      case resource:AnyRef => c.addResource(resource)
    }
    c
  }
  lazy val props = (accConf.iterator().toList:::accGeoConf.iterator().toList).map(e => (e.getKey, e.getValue)).toMap

  def get(prop: Property) = props.getOrElse(prop.name, prop.defaultValue)
  def get(prop: org.apache.accumulo.core.conf.Property) = accConf.get(prop)
}
