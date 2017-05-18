/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.process

import org.geotools.process.factory.AnnotatedBeanProcessFactory
import org.geotools.process.vector.{QueryProcess, UniqueProcess}
import org.geotools.text.Text

class ProcessFactory
    extends AnnotatedBeanProcessFactory(Text.text("GeoMesa Process Factory"), "geomesa", ProcessFactory.Processes: _*)

object ProcessFactory {
  val Processes = Seq(
    classOf[DensityProcess],
    classOf[Point2PointProcess],
//    classOf[QueryProcess],
//    classOf[UniqueProcess],
    classOf[HashAttributeProcess],
    classOf[HashAttributeColorProcess],
    classOf[ArrowConversionProcess]
  )
}
