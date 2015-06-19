/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.geotools.factory.GeoTools

object IteratorClassLoader extends Logging {

  private var initialized = false

  def initClassLoader(clas: Class[_]) = synchronized {
    if (!initialized) {
      try {
        logger.trace("Initializing classLoader")
        // locate the geomesa jars
        clas.getClassLoader match {
          case vfsCl: VFSClassLoader =>
            vfsCl.getFileObjects.map(_.getURL).filter(_.toString.contains("geomesa")).foreach { url =>
              logger.debug(s"Found geomesa jar at $url")
              val classLoader = java.net.URLClassLoader.newInstance(Array(url), vfsCl)
              GeoTools.addClassLoader(classLoader)
            }

          case _ => // no -op
        }
      } catch {
        case t: Throwable => logger.error("Failed to initialize GeoTools' ClassLoader", t)
      } finally {
        initialized = true
      }
    }
  }
}
