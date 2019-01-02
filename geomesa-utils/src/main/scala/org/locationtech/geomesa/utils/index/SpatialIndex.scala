/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}

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

  @deprecated("use insert(geometry, key, item)")
  def insert(x: Double, y: Double, key: String, item: T): Unit = insert(SpatialIndex.geometry(x, y), key, item)

  @deprecated("use insert(geometry, key, item)")
  def insert(envelope: Envelope, key: String, item: T): Unit = insert(SpatialIndex.geometry(envelope), key, item)

  @deprecated("use insert(geometry, key, item)")
  def insert(envelope: Envelope, item: T): Unit = insert(SpatialIndex.geometry(envelope), item.toString, item)

  @deprecated("use remove(geometry, key)")
  def remove(x: Double, y: Double, key: String): T = remove(SpatialIndex.geometry(x, y), key)

  @deprecated("use remove(geometry, key)")
  def remove(envelope: Envelope, key: String): T = remove(SpatialIndex.geometry(envelope), key)

  @deprecated("use remove(geometry, key)")
  def remove(envelope: Envelope, item: T): Boolean = remove(SpatialIndex.geometry(envelope), item.toString) != null

  @deprecated("use get(geometry, key)")
  def get(x: Double, y: Double, key: String): T = get(SpatialIndex.geometry(x, y), key)

  @deprecated("use get(geometry, key)")
  def get(envelope: Envelope, key: String): T = get(SpatialIndex.geometry(envelope), key)

  @deprecated("query(bbox).filter(predicate)")
  def query(envelope: Envelope, filter: T => Boolean): Iterator[T] = query(envelope).filter(filter.apply)
}

object SpatialIndex {

  private val gf = new GeometryFactory()

  @deprecated
  def getCenter(envelope: Envelope): (Double, Double) = {
    val x = (envelope.getMinX + envelope.getMaxX) / 2.0
    val y = (envelope.getMinY + envelope.getMaxY) / 2.0
    (x, y)
  }

  private def geometry(x: Double, y: Double): Geometry = gf.createPoint(new Coordinate(x, y))

  private def geometry(envelope: Envelope): Geometry = {
    val coords = Array(
      new Coordinate(envelope.getMinX, envelope.getMinY),
      new Coordinate(envelope.getMaxX, envelope.getMinY),
      new Coordinate(envelope.getMaxX, envelope.getMaxY),
      new Coordinate(envelope.getMinX, envelope.getMaxY),
      new Coordinate(envelope.getMinX, envelope.getMinY)
    )
    gf.createPolygon(coords)
  }
}
