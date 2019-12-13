/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api.handlers

import java.io.File
import java.util

import org.locationtech.geomesa.utils.text.WKTUtils

import scala.collection.JavaConversions._

class WKTFileHandler extends AbstractFileHandler {
  override def canProcess(file: File, params: util.Map[String, String]) = {
    params.contains("wkt")
  }

  override def getGeometryFromParams(params: util.Map[String, String]) = {
    Option(WKTUtils.read(params("wkt")))
  }
}
