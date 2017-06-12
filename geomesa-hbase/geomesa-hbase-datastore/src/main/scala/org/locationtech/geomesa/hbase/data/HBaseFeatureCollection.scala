/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.geotools.data._
import org.locationtech.geomesa.index.geotools.{GeoMesaFeatureCollection, GeoMesaFeatureSource}
import org.locationtech.geomesa.process.analytic.{AttributeVisitor, StatsVisitor}
import org.locationtech.geomesa.process.transform.{ArrowVisitor, BinVisitor}
import org.opengis.feature.FeatureVisitor
import org.opengis.util.ProgressListener

/**
 * Feature collection implementation
 */
class HBaseFeatureCollection(source: GeoMesaFeatureSource, query: Query)
    extends GeoMesaFeatureCollection(source, query) {

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit =
    visitor match {
      case v: AttributeVisitor => v.execute(source, query)
      case v: ArrowVisitor =>     v.execute(source, query)
      case v: BinVisitor =>       v.execute(source, query)
      case v: StatsVisitor =>     v.execute(source, query)
      case _ => super.accepts(visitor, progress)
    }
}
