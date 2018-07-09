/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine

import com.vividsolutions.jts.geom.Envelope
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.memory.cqengine.GeoCQIndexSupport.GeoCQIndex
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * SpatialIndexSupport for GeoCQEngine
  *
  * @param sft simple feature type
  * @param index index
  */
class GeoCQIndexSupport(override val sft: SimpleFeatureType, override val index: GeoCQIndex)
    extends SpatialIndexSupport {
  override def query(filter: Filter): Iterator[SimpleFeature] = index.engine.query(filter)
}

object GeoCQIndexSupport {

  def apply(sft: SimpleFeatureType, xResolution: Int, yResolution: Int): GeoCQIndexSupport = {
    val engine = new GeoCQEngine(sft, enableFidIndex = true, enableGeomIndex = true, (xResolution, yResolution))
    new GeoCQIndexSupport(sft, new GeoCQIndex(engine))
  }

  /**
    * SpatialIndex wrapper for GeoCQEngine
    *
    * @param engine geocq instance
    */
  class GeoCQIndex(val engine: GeoCQEngine) extends SpatialIndex[SimpleFeature] {

    override def insert(x: Double, y: Double, key: String, item: SimpleFeature): Unit = engine.insert(item)

    override def insert(envelope: Envelope, key: String, item: SimpleFeature): Unit = engine.insert(item)

    override def remove(x: Double, y: Double, key: String): SimpleFeature = engine.remove(key)

    override def remove(envelope: Envelope, key: String): SimpleFeature = engine.remove(key)

    override def get(x: Double, y: Double, key: String): SimpleFeature = engine.get(key)

    override def get(envelope: Envelope, key: String): SimpleFeature = engine.get(key)

    override def query(): Iterator[SimpleFeature] = engine.query(Filter.INCLUDE)

    override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[SimpleFeature] = {
      import org.locationtech.geomesa.filter.ff
      engine.query(ff.bbox(engine.sft.getGeometryDescriptor.getLocalName, xmin, ymin, xmax, ymax, "EPSG:4326"))
    }

    override def size(): Int = engine.size()

    override def clear(): Unit = engine.clear()
  }
}
