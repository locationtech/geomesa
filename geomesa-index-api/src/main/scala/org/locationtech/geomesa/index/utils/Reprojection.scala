/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.geotools.data.Query
import org.geotools.feature.FeatureTypes
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer
import org.geotools.referencing.CRS
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.referencing.crs.CoordinateReferenceSystem

/**
  * Reproject the geometries in a simple feature to a different CRS
  */
trait Reprojection {
  def apply(feature: SimpleFeature): SimpleFeature
}

object Reprojection {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  /**
    * Create a reprojection function
    *
    * @param returnSft simple feature type being returned
    * @param crs crs information from a query
    * @return
    */
  def apply(returnSft: SimpleFeatureType, crs: QueryReferenceSystems): Reprojection = {
    if (crs.target != crs.user) {
      val transformer = new GeometryCoordinateSequenceTransformer
      transformer.setMathTransform(CRS.findMathTransform(crs.user, crs.target, true))
      val transformed = FeatureTypes.transform(returnSft, crs.target) // note: drops user data
      new TransformReprojection(SimpleFeatureTypes.immutable(transformed, returnSft.getUserData), transformer)
    } else if (crs.user != crs.native) {
      val transformed = FeatureTypes.transform(returnSft, crs.user) // note: drops user data
      new UserReprojection(SimpleFeatureTypes.immutable(transformed, returnSft.getUserData))
    } else {
      throw new IllegalArgumentException(s"Trying to reproject to the same CRS: $crs")
    }
  }

  /**
    * Holds query projection info
    *
    * @param native native crs of the data
    * @param user user crs for the query (data will be treated as this crs but without any transform)
    * @param target target crs for the query (data will be transformed to this crs)
    */
  case class QueryReferenceSystems(
      native: CoordinateReferenceSystem,
      user: CoordinateReferenceSystem,
      target: CoordinateReferenceSystem)

  object QueryReferenceSystems {
    def apply(query: Query): Option[QueryReferenceSystems] = {
      Option(query.getHints.getReturnSft.getGeometryDescriptor).flatMap { descriptor =>
        val native = descriptor.getCoordinateReferenceSystem
        val source = Option(query.getCoordinateSystem).getOrElse(native)
        val target = Option(query.getCoordinateSystemReproject).getOrElse(native)
        if (target == source && source == native) { None } else {
          Some(QueryReferenceSystems(native, source, target))
        }
      }
    }
  }

  /**
    * Applies a geometric transform to any geometry attributes
    *
    * @param sft simple feature type being projected to
    * @param transformer transformer
    */
  private class TransformReprojection(sft: SimpleFeatureType, transformer: GeometryCoordinateSequenceTransformer)
      extends Reprojection {
    override def apply(feature: SimpleFeature): SimpleFeature = {
      val values = Array.tabulate(sft.getAttributeCount) { i =>
        feature.getAttribute(i) match {
          case g: Geometry => transformer.transform(g)
          case a => a
        }
      }
      new ScalaSimpleFeature(sft, feature.getID, values, feature.getUserData)
    }
  }

  /**
    * Changes the defined crs but does not do any actual geometric transforms
    *
    * @param sft simple feature type being projected to
    */
  private class UserReprojection(sft: SimpleFeatureType) extends Reprojection {
    override def apply(feature: SimpleFeature): SimpleFeature = ScalaSimpleFeature.copy(sft, feature)
  }
}
