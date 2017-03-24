/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.Writer

import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.geojson.feature.FeatureJSON

class GeoJsonExporter(writer: Writer) extends FeatureExporter {

  val featureJson = new FeatureJSON()

  override def export(features: SimpleFeatureCollection): Option[Long]  = {
    featureJson.writeFeatureCollection(features, writer)
    None
  }

  override def flush(): Unit  = writer.flush()

  override def close(): Unit  = {
    writer.flush()
    writer.close()
  }
}
