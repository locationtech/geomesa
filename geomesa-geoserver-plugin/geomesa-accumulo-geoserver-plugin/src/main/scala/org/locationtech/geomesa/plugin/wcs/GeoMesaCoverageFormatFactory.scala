/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.wcs

import java.awt.RenderingHints
import java.util.Collections

import org.geotools.coverage.grid.io.{AbstractGridFormat, GridFormatFactorySpi}

class GeoMesaCoverageFormatFactory extends GridFormatFactorySpi {
  def isAvailable = true

  def createFormat(): AbstractGridFormat = new GeoMesaCoverageFormat

  def getImplementationHints: java.util.Map[RenderingHints.Key, _] = Collections.emptyMap()

}
