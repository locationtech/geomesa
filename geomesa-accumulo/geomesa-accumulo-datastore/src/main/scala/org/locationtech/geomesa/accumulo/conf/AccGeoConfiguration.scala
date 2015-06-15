/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.conf

import org.apache.accumulo.core.conf.{AccumuloConfiguration, DefaultConfiguration}
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties

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
    val configFile = GeomesaSystemProperties.CONFIG_FILE.get
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
