/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.wcs

import java.awt.RenderingHints
import java.net.URL
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import org.geotools.coverage.grid.io.{AbstractGridFormat, GridFormatFactorySpi}

class GeoMesaCoverageFormatFactory extends GridFormatFactorySpi {

  def isAvailable = {
    // if these classes are here, then the runtine environment has
    // access to JAI and the JAI ImageI/O toolbox.
    try {
      Class.forName("javax.media.jai.JAI")
      Class.forName("com.sun.media.jai.operator.ImageReadDescriptor")
      true
    } catch {
      case c: ClassNotFoundException => false
    }
  }

  def createFormat(): AbstractGridFormat = new GeoMesaCoverageFormat

  def getImplementationHints: java.util.Map[RenderingHints.Key, _] = Collections.emptyMap()
}
