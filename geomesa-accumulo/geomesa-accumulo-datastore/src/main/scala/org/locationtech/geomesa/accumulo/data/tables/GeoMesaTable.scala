/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

trait GeoMesaTable {

  /**
   * Is the table compatible with the given feature type
   */
  def supports(sft: SimpleFeatureType): Boolean

  /**
   * The name used to identify the table
   */
  def suffix: String

  /**
   * Creates a function to write a feature to the table
   */
  def writer(sft: SimpleFeatureType): Option[FeatureToMutations]

  /**
   * Creates a function to delete a feature to the table
   */
  def remover(sft: SimpleFeatureType): Option[FeatureToMutations]

  /**
   * Deletes all features from the table
   */
  def deleteFeaturesForType(sft: SimpleFeatureType, bd: BatchDeleter): Unit = {
    val prefix = getTableSharingPrefix(sft)
    val range = new AccRange(new Text(prefix), true, AccRange.followingPrefix(new Text(prefix)), false)
    bd.setRanges(Seq(range))
    bd.delete()
  }
}

object GeoMesaTable {

  def getTablesAndNames(sft: SimpleFeatureType, acc: AccumuloConnectorCreator): Seq[(GeoMesaTable, String)] = {
    val version = acc.getGeomesaVersion(sft)
    val rec  = (RecordTable, acc.getRecordTable(sft))
    val st   = (SpatioTemporalTable, acc.getSpatioTemporalTable(sft))
    val attr = (AttributeTable, acc.getAttributeTable(sft))
    val tables = if (version < 5) {
      Seq(rec, st, attr)
    } else {
      val z3 = (Z3Table, acc.getZ3Table(sft))
      Seq(rec, z3, st, attr)
    }
    tables.filter(_._1.supports(sft))
  }
}