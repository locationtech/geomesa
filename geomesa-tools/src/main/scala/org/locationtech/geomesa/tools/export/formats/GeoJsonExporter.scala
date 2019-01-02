/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.Writer

import org.geotools.geojson.feature.FeatureJSON
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoJsonExporter(writer: Writer) extends FeatureExporter {

  private val json = new FeatureJSON()

  private var first = true

  override def start(sft: SimpleFeatureType): Unit = writer.write("""{"type":"FeatureCollection","features":[""")

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    features.foreach { feature =>
      if (first) {
        first = false
      } else {
        writer.write(",")
      }
      json.writeFeature(feature, writer)
      count += 1L
    }
    writer.flush()
    Some(count)
  }

  override def close(): Unit  = {
    writer.write("]}\n")
    writer.close()
  }
}
