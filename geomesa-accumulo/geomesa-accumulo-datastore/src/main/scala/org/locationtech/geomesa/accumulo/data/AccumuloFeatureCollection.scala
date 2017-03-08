/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.locationtech.geomesa.accumulo.process.{RouteVisitor, SamplingVisitor}
import org.locationtech.geomesa.accumulo.process.knn.KNNVisitor
import org.locationtech.geomesa.accumulo.process.proximity.ProximityVisitor
import org.locationtech.geomesa.accumulo.process.query.QueryVisitor
import org.locationtech.geomesa.accumulo.process.stats.StatsVisitor
import org.locationtech.geomesa.accumulo.process.tube.TubeVisitor
import org.locationtech.geomesa.accumulo.process.unique.AttributeVisitor
import org.locationtech.geomesa.index.geotools.{GeoMesaFeatureCollection, GeoMesaFeatureSource}
import org.opengis.feature.FeatureVisitor
import org.opengis.util.ProgressListener

/**
 * Feature collection implementation
 */
class AccumuloFeatureCollection(source: GeoMesaFeatureSource, query: Query)
    extends GeoMesaFeatureCollection(source, query) {

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit =
    visitor match {
      case v: TubeVisitor      => v.setValue(v.tubeSelect(source, query))
      case v: ProximityVisitor => v.setValue(v.proximitySearch(source, query))
      case v: QueryVisitor     => v.setValue(v.query(source, query))
      case v: StatsVisitor     => v.setValue(v.query(source, query))
      case v: SamplingVisitor  => v.setValue(v.sample(source, query))
      case v: KNNVisitor       => v.setValue(v.kNNSearch(source,query))
      case v: AttributeVisitor => v.setValue(v.unique(source, query))
      case v: RouteVisitor     => v.routeSearch(source, query)
      case _ => super.accepts(visitor, progress)
    }
}