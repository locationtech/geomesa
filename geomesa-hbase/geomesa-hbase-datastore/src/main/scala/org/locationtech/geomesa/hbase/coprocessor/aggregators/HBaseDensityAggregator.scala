/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators

import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.coprocessor.SFT_OPT
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils._
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable

class HBaseDensityAggregator extends GeoMesaHBaseAggregator with KryoLazyDensityUtils {
  var densityResult: DensityResult = _

  override def init(options: Map[String, String]): Unit = {
    val sft = SimpleFeatureTypes.createType("input", options(SFT_OPT))
    densityResult = initialize(options, sft)
  }

  override def aggregate(sf: SimpleFeature): Unit = writeGeom(sf, densityResult)

  override def encodeResult(): Array[Byte] = KryoLazyDensityUtils.encodeResult(densityResult)
}

object HBaseDensityAggregator {

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    sf.setAttribute(0, GeometryUtils.zeroPoint)
    sf.getUserData.put(DENSITY_VALUE, bytes)
    sf
  }

  /**
    * Creates an iterator config for the kryo density iterator
    */
  def configure(sft: SimpleFeatureType,
                hints: Hints): Map[String, String] = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val is = mutable.Map.empty[String, String]

    val envelope = hints.getDensityEnvelope.get
    val (width, height) = hints.getDensityBounds.get

    is.put(GeoMesaHBaseAggregator.AGGREGATOR_CLASS, classOf[HBaseDensityAggregator].getName)
    is.put(ENVELOPE_OPT, s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}")
    is.put(GRID_OPT, s"$width,$height")
    hints.getDensityWeight.foreach(is.put(WEIGHT_OPT, _))
    is.put(SFT_OPT, SimpleFeatureTypes.encodeType(sft, includeUserData = true))

    is.toMap
  }
}
