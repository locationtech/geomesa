/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine

import com.googlecode.cqengine.attribute.Attribute
import org.locationtech.geomesa.memory.cqengine.index.param.{BucketIndexParam, GeoIndexParams, STRtreeIndexParam}
import org.locationtech.geomesa.memory.cqengine.index.{AbstractGeoIndex, BucketGeoIndex, GeoIndexType, QuadTreeGeoIndex, STRtreeGeoIndex}
import org.opengis.feature.simple.SimpleFeatureType
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeature


object GeoIndexFactory{


  import org.locationtech.geomesa.utils.conversions.JavaConverters._
  def onAttribute[A <: Geometry, O <: SimpleFeature](sft: SimpleFeatureType, attribute: Attribute[O, A], geoIndexType: GeoIndexType, geoIndexParams: Option[GeoIndexParams]): AbstractGeoIndex[A, O] = {
    val geomAttributeIndex = sft.indexOf(attribute.getAttributeName)
    val attributeDescriptor = sft.getDescriptor(geomAttributeIndex)
    checkParams(geoIndexType, geoIndexParams)
    geoIndexType match {
      case GeoIndexType.Bucket =>
        return new BucketGeoIndex[A, O](sft, attribute, geoIndexParams.map(p => p.asInstanceOf[BucketIndexParam]).asJava)
      case GeoIndexType.STRtree =>
        return new STRtreeGeoIndex[A, O](sft, attribute, geoIndexParams.map(p => p.asInstanceOf[STRtreeIndexParam]).asJava)
      case GeoIndexType.QuadTree =>
        return new QuadTreeGeoIndex[A, O](sft, attribute)
    }
    throw new IllegalArgumentException("UNKNOWN GEO INDEX TYPE")
  }

  private def checkParams(geoIndexType: GeoIndexType, geoIndexParams: Option[_ <: GeoIndexParams]): Unit = {
    if (geoIndexParams != null && geoIndexParams.isDefined && geoIndexParams.get.getGeoIndexType != geoIndexType)
      throw new IllegalArgumentException("Index type and parameters does not match")
  }
}
