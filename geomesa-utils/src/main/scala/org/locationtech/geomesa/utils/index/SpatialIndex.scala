/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import org.locationtech.jts.geom.{Envelope, Geometry}

/**
 * Trait for indexing and querying spatial data
 */
trait SpatialIndex[T] {

  /**
    * Insert a value indexed by a geometry and unique key
    *
    * @param geom geometry used for indexing
    * @param key unique value key
    * @param value value to store
    */
  def insert(geom: Geometry, key: String, value: T): Unit

  /**
    * Remove a value based on its indexed geometry and unique key
    *
    * @param geom geometry used for indexing
    * @param key unique value key
    * @return value, if it exists, or null
    */
  def remove(geom: Geometry, key: String): T

  /**
    * Retrieves a value by primary key
    *
    * @param geom geometry used for indexing
    * @param key unique value key
    * @return value, if it exists, or null
    */
  def get(geom: Geometry, key: String): T

  /**
    * Query based on a bounding box
    *
    * @param xmin xmin
    * @param ymin ymin
    * @param xmax xmax
    * @param ymax ymax
    * @return
    */
  def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T]

  /**
    * Query based on a bounding envelope
    *
    * @param bbox bbox
    * @return
    */
  def query(bbox: Envelope): Iterator[T] = query(bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY)

  /**
    * Return all items
    *
    * @return
    */
  def query(): Iterator[T]

  /**
    * Number of items in this index
    *
    * @return
    */
  def size(): Int

  /**
    * Remove all items from the index
    */
  def clear(): Unit
}
