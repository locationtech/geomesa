/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.io.{Closeable, Flushable}

import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Interface between generic methods and back-end specific code
  *
  * @tparam DS data store binding
  */
trait IndexAdapter[DS <: GeoMesaDataStore[DS]] {

  val groups: ColumnGroups = new ColumnGroups

  /**
    * Create a table
    *
    * @param index index
    * @param partition table partition
    * @param splits splits
    */
  def createTable(index: GeoMesaFeatureIndex[_, _], partition: Option[String], splits: => Seq[Array[Byte]]): Unit

  /**
    * Delete a table
    *
    * @param tables table names
    */
  def deleteTables(tables: Seq[String]): Unit

  /**
    * Truncate a table
    *
    * @param tables tables
    * @param prefix prefix filter, or none for all rows
    */
  def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit

  /**
    * Writer for the given feature indices
    *
    * @param sft simple feature type
    * @param indices indices
    * @param partition partition to write, if any
    * @return
    */
  def createWriter(sft: SimpleFeatureType,
                   indices: Seq[GeoMesaFeatureIndex[_, _]],
                   partition: Option[String] = None): IndexWriter

  /**
    * Create a query plan
    *
    * @param strategy strategy
    * @return
    */
  def createQueryPlan(strategy: QueryStrategy): QueryPlan[DS]
}

object IndexAdapter {

  /**
    * Writes features to a particular back-end data store implementation
    *
    * @param indices indices being written to
    * @param wrapper creates writable feature
    */
  abstract class IndexWriter(val indices: Seq[GeoMesaFeatureIndex[_, _]], wrapper: FeatureWrapper)
      extends Closeable with Flushable {

    private val converters = indices.map(_.createConverter()).toArray
    private val values = Array.ofDim[RowKeyValue[_]](indices.length)

    private var i = 0

    /**
      * Write the feature. This method should ensure that the feature is not partially written, by first
      * validating that all of the indices can index it successfully
      *
      * @param feature feature
      */
    def write(feature: SimpleFeature): Unit = {
      val writable = wrapper.wrap(feature)

      i = 0
      // calculate all the mutations up front to ensure that there aren't any validation errors
      while (i < converters.length) {
        values(i) = converters(i).convert(writable)
        i +=1
      }

      write(writable, values)
    }

    /**
      * Delete the feature
      *
      * @param feature feature
      */
    def delete(feature: SimpleFeature): Unit = {
      val writable = wrapper.wrap(feature)

      i = 0
      // we assume that all converters will pass as this feature was already written once
      while (i < converters.length) {
        values(i) = converters(i).convert(writable, lenient = true)
        i += 1
      }

      delete(writable, values)
    }

    /**
      * Write values derived from the feature
      *
      * @param feature feature being written
      * @param values derived values, one per index
      */
    protected def write(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit

    /**
      * Delete values derived from the feature
      *
      * @param feature feature being deleted
      * @param values derived values, one per index
      */
    protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit
  }
}
