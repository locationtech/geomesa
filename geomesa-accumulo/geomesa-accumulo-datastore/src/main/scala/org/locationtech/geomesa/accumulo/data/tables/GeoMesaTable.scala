/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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