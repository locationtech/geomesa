/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geojson

import java.io.Closeable

trait GeoJsonIndex {

  /**
    * Create a new index by name
    *
    * @param name identifier for this index
    * @param id json-path expression to extract an ID from a geojson feature, used to uniquely identify that feature
    * @param dtg json-path expression to extract a date from a geojson feature, used to index by date
    * @param points store the geojson geometries as points or as geometries with extents
    */
  def createIndex(name: String, id: Option[String] = None, dtg: Option[String] = None, points: Boolean = false): Unit

  /**
    * Delete an existing index by name
    *
    * @param name index to delete
    */
  def deleteIndex(name: String): Unit

  /**
    * Add new features to the index
    *
    * @param name index to modify
    * @param json geojson 'Feature' or 'FeatureCollection'
    * @return ids of the newly created features
    */
  def add(name: String, json: String): Seq[String]

  /**
    * Update existing features in the index. To use this method, the index must have
    * been created with an 'id' json-path in order to determine which features to update
    *
    * @param name index to modify
    * @param json geojson 'Feature' or 'FeatureCollection'
    */
  def update(name: String, json: String): Unit

  /**
    * Update existing features in the index
    *
    * @param name index to modify
    * @param ids ids of the features to update - must correspond to the feature geojson
    * @param json geojson 'Feature' or 'FeatureCollection'
    */
  def update(name: String, ids: Seq[String], json: String): Unit

  /**
    * Delete an existing feature
    *
    * @param name index to modify
    * @param id id of a feature to delete
    */
  def delete(name: String, id: String): Unit = delete(name, Seq(id))

  /**
    * Delete existing features
    *
    * @param name index to modify
    * @param ids ids of features to delete
    */
  def delete(name: String, ids: Iterable[String]): Unit

  /**
    * Returns features by id
    *
    * @param name index to query
    * @param ids feature ids
    * @return
    */
  def get(name: String, ids: Iterable[String], transform: Map[String, String] = Map.empty): Iterator[String] with Closeable

  /**
    * Query features in the index
    *
    * @param name index to query
    * @param query json query string - @see org.locationtech.geomesa.geojson.query.GeoJsonQuery
    * @param transform optional transform for json coming back, consisting of path keys and json-path selector values
    * @return matching geojson, or transformed json
    */
  def query(name: String, query: String, transform: Map[String, String] = Map.empty): Iterator[String] with Closeable
}
