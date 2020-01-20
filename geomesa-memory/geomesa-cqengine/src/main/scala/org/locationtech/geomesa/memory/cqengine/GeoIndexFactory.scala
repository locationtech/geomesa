/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine

import com.googlecode.cqengine.attribute.Attribute
import org.locationtech.geomesa.memory.cqengine.index.param.{BucketIndexParam, GeoIndexParams, STRtreeIndexParam}
import org.locationtech.geomesa.memory.cqengine.index._
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object GeoIndexFactory{

  def onAttribute[A <: Geometry, O <: SimpleFeature](
      sft: SimpleFeatureType,
      attribute: Attribute[O, A],
      geoIndexType: GeoIndexType,
      geoIndexParams: Option[GeoIndexParams]): AbstractGeoIndex[A, O] = {

    if (geoIndexParams.exists(_.getGeoIndexType != geoIndexType)) {
      throw new IllegalArgumentException("Index type and parameters does not match")
    }

    geoIndexType match {
      case GeoIndexType.Bucket =>
        geoIndexParams match {
          case Some(p: BucketIndexParam) => new BucketGeoIndex[A, O](sft, attribute, p)
          case _ => new BucketGeoIndex[A, O](sft, attribute)
        }

      case GeoIndexType.STRtree =>
        geoIndexParams match {
          case Some(p: STRtreeIndexParam) => new STRtreeGeoIndex[A, O](sft, attribute, p)
          case _ => new STRtreeGeoIndex[A, O](sft, attribute)
        }

      case GeoIndexType.QuadTree =>
        new QuadTreeGeoIndex[A, O](sft, attribute)

      case _ => throw new IllegalArgumentException(s"Unexpected geo-index-type: $geoIndexType")
    }
  }
}
