/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.data.tables

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.data
import org.apache.accumulo.core.data.Key
import org.locationtech.geomesa.accumulo
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.index.{IndexSchema, _}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object SpatioTemporalTable extends GeoMesaTable with Logging {

  val INDEX_FLAG = "0"
  val DATA_FLAG = "1"

  val INDEX_CHECK = s"~$INDEX_FLAG~"
  val DATA_CHECK = s"~$DATA_FLAG~"

  override def supports(sft: SimpleFeatureType): Boolean = true

  override val suffix: String = "st_idx"

  override def writer(sft: SimpleFeatureType): Option[FeatureToMutations] = {
    val indexSchema = sft.getUserData.get(accumulo.index.SFT_INDEX_SCHEMA).asInstanceOf[String]
    val stEncoder = IndexSchema.buildKeyEncoder(sft, indexSchema)
    Some((toWrite: FeatureToWrite) => stEncoder.encode(toWrite))
  }

  override def remover(sft: SimpleFeatureType): Option[FeatureToMutations] = {
    val indexSchema = sft.getUserData.get(accumulo.index.SFT_INDEX_SCHEMA).asInstanceOf[String]
    val stEncoder = IndexSchema.buildKeyEncoder(sft, indexSchema)
    Some((toWrite: FeatureToWrite) => stEncoder.encode(toWrite, delete = true))
  }

  // index rows have an index flag as part of the schema
  def isIndexEntry(key: Key): Boolean = key.getRow.find(INDEX_CHECK) != -1

  // data rows have a data flag as part of the schema
  def isDataEntry(key: Key): Boolean = key.getRow.find(DATA_CHECK) != -1

  override def deleteFeaturesForType(sft: SimpleFeatureType, bd: BatchDeleter): Unit = {
    val MIN_START = "\u0000"
    val MAX_END = "~"

    val schema = getIndexSchema(sft).getOrElse {
      val msg = s"Cannot delete ${sft.getTypeName}. SFT does not have its index schema stored."
      throw new Exception(msg)
    }

    val (rowf, _,_) = IndexSchema.parse(IndexSchema.formatter, schema).get
    val planners = rowf.lf match {
      case Seq(pf: PartitionTextFormatter, i: IndexOrDataTextFormatter, const: ConstantTextFormatter, r@_*) =>
        // Build ranges using pf, ip and const!
        val rpp = RandomPartitionPlanner(pf.numPartitions)
        val ip = IndexOrDataPlanner()
        val csp = ConstStringPlanner(const.constStr)
        Seq(rpp, ip, csp)

      case _ =>
        throw new RuntimeException(s"Cannot delete ${sft.getTypeName}. SFT has an invalid schema structure.")
    }

    val planner =  CompositePlanner(planners, "~")
    val keyPlans =
      Seq(true, false).map(indexOnly => planner.getKeyPlan(AcceptEverythingFilter, indexOnly, ExplainNull))

    val ranges = keyPlans.flatMap {
      case KeyRanges(rs) => rs.map(r => new data.Range(r.start + "~" + MIN_START, r.end + "~" + MAX_END))
      case _ =>
        logger.error(s"Keyplanner failed to build range properly.")
        Seq.empty
    }

    bd.setRanges(ranges)
    bd.delete()
    bd.close()
  }
}
