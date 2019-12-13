/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process

import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.visitor.AbstractCalcResult
import org.geotools.process.vector.VectorProcess

/**
  * Marker trait for dynamic loading of processes
  */
trait GeoMesaProcess extends VectorProcess

/**
  * Common trait for visitors, allows for feature collections to execute processing in a standardized way
  */
@deprecated("Replaced with org.locationtech.geomesa.index.process.GeoMesaProcessVisitor")
trait GeoMesaProcessVisitor extends org.locationtech.geomesa.index.process.GeoMesaProcessVisitor

case class FeatureResult(results: SimpleFeatureCollection) extends AbstractCalcResult
