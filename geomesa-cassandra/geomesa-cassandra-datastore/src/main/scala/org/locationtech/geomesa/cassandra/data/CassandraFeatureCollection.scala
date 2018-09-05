/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import org.geotools.data.Query
import org.locationtech.geomesa.index.geotools.{GeoMesaFeatureCollection, GeoMesaFeatureSource}
import org.locationtech.geomesa.process.query.QueryVisitor
import org.opengis.feature.FeatureVisitor
import org.opengis.util.ProgressListener

class CassandraFeatureCollection(source: GeoMesaFeatureSource, query: Query)
    extends GeoMesaFeatureCollection(source, query) {

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit =
    visitor match {
      case v: QueryVisitor => v.execute(source, query)
      case _ => super.accepts(visitor, progress)
    }
}
