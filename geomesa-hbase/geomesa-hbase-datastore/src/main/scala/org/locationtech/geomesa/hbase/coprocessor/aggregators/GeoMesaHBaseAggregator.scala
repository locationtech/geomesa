/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators

import org.opengis.feature.simple.SimpleFeature

trait GeoMesaHBaseAggregator {
  def init(options: Map[String, String]): Unit
  def aggregate(sf: SimpleFeature): Unit
  def encodeResult(): Array[Byte]
}

object GeoMesaHBaseAggregator {
  val AGGREGATOR_CLASS = "geomesa.hbase.aggregator.class"
}