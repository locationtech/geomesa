/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

object GeoMesaProperties extends LazyLogging {

  private val EmbeddedFile = "/org/locationtech/geomesa/geomesa.properties"

  private val props: Properties = {
    val resource = getClass.getResourceAsStream(EmbeddedFile)
    val props = new Properties
    if (resource == null) {
      logger.warn(s"Couldn't load $EmbeddedFile")
      props
    } else {
      try {
        props.load(resource)
      } finally {
        resource.close()
      }
      props
    }
  }

  val ProjectVersion = props.getProperty("geomesa.project.version")
  val BuildDate      = props.getProperty("geomesa.build.date")
  val GitCommit      = props.getProperty("geomesa.build.commit.id")
  val GitBranch      = props.getProperty("geomesa.build.branch")
}
