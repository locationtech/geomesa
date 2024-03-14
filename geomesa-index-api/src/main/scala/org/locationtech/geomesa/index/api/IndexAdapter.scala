/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.security.VisibilityChecker

import java.io.{Closeable, Flushable}
import java.util.UUID
import scala.util.control.NonFatal

/**
  * Interface between generic methods and back-end specific code
  *
  * @tparam DS data store binding
  */
trait IndexAdapter[DS <: GeoMesaDataStore[DS]] {

  val groups: ColumnGroups = new ColumnGroups

  val tableNameLimit: Option[Int] = None

  /**
    * Create a table
    *
    * @param index index
    * @param partition table partition
    * @param splits splits
    */
  def createTable(index: GeoMesaFeatureIndex[_, _], partition: Option[String], splits: => Seq[Array[Byte]]): Unit

  /**
    * Rename a table
    *
    * @param from current table name
    * @param to new table name
    */
  def renameTable(from: String, to: String): Unit

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
  def createWriter(
      sft: SimpleFeatureType,
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      partition: Option[String] = None,
      atomic: Boolean = false): IndexWriter

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
    * Checks a table name for a max limit. If the table name exceeds the limit, then it will be
    * truncated and a UUID appended. Note that if the limit is less than 34 (one char prefix,
    * an underscore separator, and 32 chars for a UUID), this method will throw an exception
    *
    * @param name desired name
    * @param limit database limit on the length of a table name
    * @return
    */
  @throws[IllegalArgumentException]("Limit does not fit a UUID (34 chars)")
  def truncateTableName(name: String, limit: Int): String = {
    val offset = limit - 33
    if (offset <= 0) {
      throw new IllegalArgumentException(s"Limit is too small to fit a UUID, must be at least 34 chars: $limit")
    }
    s"${name.substring(0, offset)}_${UUID.randomUUID().toString.replaceAllLiterally("-", "")}"
  }

  trait IndexWriter extends Closeable with Flushable {

    /**
      * Write the feature. This method should ensure that the feature is not partially written, by first
      * validating that all of the indices can index it successfully
      *
      * @param feature feature
      */
    def append(feature: SimpleFeature): Unit

    /**
     * Write the feature, replacing a previous version. This method should ensure that the feature is
     * not partially written, by first validating that all of the indices can index it successfully
     *
     * @param updated new feature
     * @param previous old feature that should be replaced
     */
    def update(updated: SimpleFeature, previous: SimpleFeature): Unit

    /**
      * Delete the feature
      *
      * @param feature feature
      */
    def delete(feature: SimpleFeature): Unit
  }

  /**
   * Writes features to a particular back-end data store implementation
   *
   * @param indices indices being written to
   * @param wrapper creates writable feature
   */
  abstract class BaseIndexWriter[T <: WritableFeature](
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      wrapper: FeatureWrapper[T]
    ) extends IndexWriter {

    private val converters = indices.map(_.createConverter()).toArray
    private val values = Array.ofDim[RowKeyValue[_]](indices.length)
    private val previousValues = Array.ofDim[RowKeyValue[_]](indices.length)

    override def append(feature: SimpleFeature): Unit = {
      val writable = wrapper.wrap(feature)

      try {
        var i = 0
        // calculate all the mutations up front to ensure that there aren't any validation errors
        while (i < converters.length) {
          values(i) = converters(i).convert(writable)
          i +=1
        }
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException("Error creating keys for insert:", e)
      }

      append(writable, values)
    }

    override def update(updated: SimpleFeature, previous: SimpleFeature): Unit = {
      val writable = wrapper.wrap(updated)
      val removable = wrapper.wrap(previous)

      try {
        var i = 0
        // calculate all the mutations up front to ensure that there aren't any validation errors
        while (i < converters.length) {
          values(i) = converters(i).convert(writable)
          previousValues(i) = converters(i).convert(removable)
          i +=1
        }
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException("Error creating keys for insert:", e)
      }

      update(writable, values, removable, previousValues)
    }

    override def delete(feature: SimpleFeature): Unit = {
      val writable = wrapper.wrap(feature, delete = true)

      try {
        var i = 0
        // we assume that all converters will pass as this feature was already written once
        while (i < converters.length) {
          values(i) = converters(i).convert(writable, lenient = true)
          i += 1
        }
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException("Error creating keys for delete:", e)
      }

      delete(writable, values)
    }

    /**
      * Write values derived from the feature
      *
      * @param feature feature being written
      * @param values derived values, one per index
      */
    protected def append(feature: T, values: Array[RowKeyValue[_]]): Unit

    /**
     * Write values derived from the feature
     *
     * @param feature feature being written
     * @param values derived values, one per index
     * @param previous the previous feature being updated/replaced
     * @param previousValues derived values for the previous feature
     */
    protected def update(feature: T, values: Array[RowKeyValue[_]], previous: T, previousValues: Array[RowKeyValue[_]]): Unit

    /**
      * Delete values derived from the feature
      *
      * @param feature feature being deleted
      * @param values derived values, one per index
      */
    protected def delete(feature: T, values: Array[RowKeyValue[_]]): Unit
  }

  /**
   * Mixin trait to require visibilities on write
   */
  trait RequiredVisibilityWriter extends IndexWriter with VisibilityChecker {
    abstract override def append(feature: SimpleFeature): Unit = {
      requireVisibilities(feature)
      super.append(feature)
    }
    abstract override def update(feature: SimpleFeature, previous: SimpleFeature): Unit = {
      requireVisibilities(feature)
      super.update(feature, previous)
    }
    abstract override def delete(feature: SimpleFeature): Unit = {
      requireVisibilities(feature)
      super.delete(feature)
    }
  }
}
