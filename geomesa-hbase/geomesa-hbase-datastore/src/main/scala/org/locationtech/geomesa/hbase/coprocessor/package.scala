/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase

import java.io._

import org.apache.hadoop.hbase.Coprocessor
import org.locationtech.geomesa.index.api.QueryPlan.FeatureReducer
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature

package object coprocessor {

  lazy val AllCoprocessors: Seq[Class[_ <: Coprocessor]] = Seq(
    classOf[GeoMesaCoprocessor]
  )

  case class CoprocessorConfig(
      options: Map[String, String],
      bytesToFeatures: Array[Byte] => SimpleFeature,
      reduce: Option[FeatureReducer] = None)
}
