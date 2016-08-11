/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.conf

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

object GeoMesaProperties extends LazyLogging {

  private val EmbeddedFile = "/org/locationtech/geomesa/geomesa.properties"

  private val props: Properties = {
    val resource = getClass.getResourceAsStream(EmbeddedFile)
    if (resource == null) {
      logger.warn(s"Couldn't load $EmbeddedFile")
      new Properties
    } else {
      val p = new Properties
      try {
        p.load(resource)
      } finally {
        resource.close()
      }
      p
    }
  }

  val GeoMesaProjectVersion = props.getProperty("geomesa.project.version")
}
