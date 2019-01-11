/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.locationtech.jts.geom.Geometry
import org.geotools.data.Query
import org.geotools.feature.FeatureTypes
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer
import org.geotools.referencing.CRS
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class Reprojection private (sft: SimpleFeatureType, transformer: Option[GeometryCoordinateSequenceTransformer]) {
  def reproject(feature: SimpleFeature): SimpleFeature = {
    val values = Array.tabulate(sft.getAttributeCount) { i =>
      feature.getAttribute(i) match {
        case g: Geometry => transformer.map(_.transform(g)).getOrElse(g)
        case a => a
      }
    }
    val result = ScalaSimpleFeature.create(sft, feature.getID, values: _*)
    result.getUserData.putAll(feature.getUserData)
    result
  }
}

object Reprojection {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  def apply(query: Query): Option[Reprojection] = {
    val sft = query.getHints.getReturnSft
    if (sft.getGeometryDescriptor == null) { None } else {
      val native = sft.getGeometryDescriptor.getCoordinateReferenceSystem
      val source = Option(query.getCoordinateSystem).getOrElse(native)
      val target = Option(query.getCoordinateSystemReproject).getOrElse(native)

      if (target != source) {
        val transformer = new GeometryCoordinateSequenceTransformer
        transformer.setMathTransform(CRS.findMathTransform(source, target, true))
        val reprojected = FeatureTypes.transform(sft, target)
        reprojected.getUserData.putAll(sft.getUserData)
        Some(new Reprojection(reprojected, Some(transformer)))
      } else if (source != native) {
        val reprojected = FeatureTypes.transform(sft, source)
        reprojected.getUserData.putAll(sft.getUserData)
        Some(new Reprojection(reprojected, None))
      } else {
        None
      }
    }
  }
}
