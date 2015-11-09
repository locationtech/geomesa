/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.index.{IndexSchema, _}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object SpatioTemporalTable extends GeoMesaTable with Logging {

  val INDEX_FLAG = "0"
  val DATA_FLAG = "1"

  val INDEX_CHECK = s"~$INDEX_FLAG~"
  val DATA_CHECK = s"~$DATA_FLAG~"

  override def supports(sft: SimpleFeatureType): Boolean = true

  override val suffix: String = "st_idx"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val stEncoder = IndexSchema.buildKeyEncoder(sft, sft.getStIndexSchema)
    (toWrite: FeatureToWrite) => stEncoder.encode(toWrite)
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val stEncoder = IndexSchema.buildKeyEncoder(sft, sft.getStIndexSchema)
    (toWrite: FeatureToWrite) => stEncoder.encode(toWrite, delete = true)
  }

  // index rows have an index flag as part of the schema
  def isIndexEntry(key: Key): Boolean = key.getRow.find(INDEX_CHECK) != -1

  // data rows have a data flag as part of the schema
  def isDataEntry(key: Key): Boolean = key.getRow.find(DATA_CHECK) != -1

  override def deleteFeaturesForType(sft: SimpleFeatureType, bd: BatchDeleter): Unit = {
    val MIN_START = "\u0000"
    val MAX_END = "~"

    val schema = Option(sft.getStIndexSchema).getOrElse {
      throw new IllegalStateException(s"Can't delete ${sft.getTypeName} - missing index schema.")
    }

    val (rowf, _,_) = IndexSchema.parse(IndexSchema.formatter, schema).get
    val planners = rowf.lf match {
      case Seq(pf: PartitionTextFormatter, i: IndexOrDataTextFormatter, const: ConstantTextFormatter, r@_*) =>
        // Build ranges using pf, ip and const!
        val rpp = RandomPartitionPlanner(pf.numPartitions + 1)
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


  override def configureTable(sft: SimpleFeatureType, tableName: String, tableOps: TableOperations): Unit = {
    val maxShard = IndexSchema.maxShard(sft.getStIndexSchema)
    val splits = (1 to maxShard - 1).map(i => new Text(s"%0${maxShard.toString.length}d".format(i)))
    tableOps.addSplits(tableName, new java.util.TreeSet(splits))

    // enable the column-family functor
    tableOps.setProperty(tableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[ColumnFamilyFunctor].getCanonicalName)
    tableOps.setProperty(tableName, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    tableOps.setProperty(tableName, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
    tableOps.setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
  }

}
