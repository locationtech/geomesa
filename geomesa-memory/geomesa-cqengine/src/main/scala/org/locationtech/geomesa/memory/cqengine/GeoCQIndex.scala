/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType.CQIndexType
import org.locationtech.geomesa.memory.index.SimpleFeatureSpatialIndex
import org.locationtech.jts.geom.Geometry

/**
  * SpatialIndex wrapper for GeoCQEngine
  *
  * @param engine geocq instance
  */
class GeoCQIndex(val engine: GeoCQEngine) extends SimpleFeatureSpatialIndex {

  def this(sft: SimpleFeatureType, attributes: Seq[(String, CQIndexType)], xResolution: Int, yResolution: Int) =
    this(new GeoCQEngine(sft, attributes, enableFidIndex = true, (xResolution, yResolution)))

  override def sft: SimpleFeatureType = engine.sft

  override def insert(geom: Geometry, key: String, value: SimpleFeature): Unit = engine.insert(value)

  override def remove(geom: Geometry, key: String): SimpleFeature = engine.remove(key)

  override def get(geom: Geometry, key: String): SimpleFeature = engine.get(key)

  override def query(): Iterator[SimpleFeature] = engine.query(Filter.INCLUDE)

  override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.filter.ff
    engine.query(ff.bbox(engine.sft.getGeometryDescriptor.getLocalName, xmin, ymin, xmax, ymax, "EPSG:4326"))
  }

  override def query(filter: Filter): Iterator[SimpleFeature] = engine.query(filter)

  override def size(): Int = engine.size()

  override def clear(): Unit = engine.clear()
}
