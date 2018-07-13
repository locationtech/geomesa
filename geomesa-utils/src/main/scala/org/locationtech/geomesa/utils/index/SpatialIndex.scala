/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Envelope

/**
 * Trait for indexing and querying spatial data
 */
trait SpatialIndex[T] {

  /**
    * Insert a point item
    *
    * @param x x coordinate
    * @param y y coordinate
    * @param key key
    * @param item item
    */
  def insert(x: Double, y: Double, key: String, item: T): Unit

  /**
    * Insert an item with extents
    *
    * @param envelope envelope
    * @param key key
    * @param item item
    */
  def insert(envelope: Envelope, key: String, item: T): Unit

  /**
    * Remove an item by location and key
    *
    * @param x x coordinate
    * @param y y coordinate
    * @param key key
    * @return the item, if it existed
    */
  def remove(x: Double, y: Double, key: String): T

  /**
    * Remove an item by location and key
    *
    * @param envelope envelope
    * @param key key
    */
  def remove(envelope: Envelope, key: String): T

  /**
    * Retrieve an item by location and key
    *
    * @param x x coordinate
    * @param y y coordinate
    * @param key key
    * @return
    */
  def get(x: Double, y: Double, key: String): T

  /**
    * Retrieves an item by location and key
    *
    * @param envelope envelope
    * @param key key
    */
  def get(envelope: Envelope, key: String): T

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

  @deprecated("insert(envelope, key, item)")
  def insert(envelope: Envelope, item: T): Unit = insert(envelope, item.toString, item)

  @deprecated("remove(envelope, key, item)")
  def remove(envelope: Envelope, item: T): Boolean = remove(envelope, item.toString) != null

  @deprecated("query(bbox).filter(predicate)")
  def query(envelope: Envelope, filter: (T) => Boolean): Iterator[T] = query(envelope).filter(filter.apply)
}

object SpatialIndex {
  @deprecated
  def getCenter(envelope: Envelope): (Double, Double) = {
    val x = (envelope.getMinX + envelope.getMaxX) / 2.0
    val y = (envelope.getMinY + envelope.getMaxY) / 2.0
    (x, y)
  }
}
