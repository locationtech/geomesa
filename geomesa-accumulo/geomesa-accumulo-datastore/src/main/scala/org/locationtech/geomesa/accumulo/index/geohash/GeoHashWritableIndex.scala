/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.geohash

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToMutations
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.index.utils.ExplainNull
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.Try

trait GeoHashWritableIndex extends AccumuloWritableIndex with LazyLogging {

  val INDEX_FLAG = "0"
  val DATA_FLAG = "1"

  val INDEX_CHECK = s"~$INDEX_FLAG~"
  val DATA_CHECK = s"~$DATA_FLAG~"

  override def writer(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val stEncoder = IndexSchema.buildKeyEncoder(sft, sft.getStIndexSchema)
    (toWrite: WritableFeature) => stEncoder.encode(toWrite)
  }

  override def remover(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val stEncoder = IndexSchema.buildKeyEncoder(sft, sft.getStIndexSchema)
    (toWrite: WritableFeature) => stEncoder.encode(toWrite, delete = true)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = throw new NotImplementedError

  // index rows have an index flag as part of the schema
  def isIndexEntry(key: Key): Boolean = key.getRow.find(INDEX_CHECK) != -1

  // data rows have a data flag as part of the schema
  def isDataEntry(key: Key): Boolean = key.getRow.find(DATA_CHECK) != -1

  override def removeAll(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit = {
    val table = ops.getTableName(sft.getTypeName, this)
    if (ops.tableOps.exists(table)) {
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

      val auths = ops.authProvider.getAuthorizations
      val config = GeoMesaBatchWriterConfig().setMaxWriteThreads(ops.config.writeThreads)
      val deleter = ops.connector.createBatchDeleter(table, auths, ops.config.queryThreads, config)
      try {
        deleter.setRanges(ranges)
        deleter.delete()
      } finally {
        deleter.close()
      }
    }
  }

  override def configure(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit = {
    val table = Try(ops.getTableName(sft.getTypeName, this)).getOrElse {
      val table = GeoMesaTable.formatTableName(ops.catalogTable, tableSuffix, sft)
      ops.metadata.insert(sft.getTypeName, tableNameKey, table)
      table
    }
    AccumuloVersion.ensureTableExists(ops.connector, table)
    val maxShard = IndexSchema.maxShard(sft.getStIndexSchema)
    val splits = (1 until maxShard).map(i => new Text(s"%0${maxShard.toString.length}d".format(i)))
    ops.tableOps.addSplits(table, new java.util.TreeSet(splits))

    // enable the column-family functor
    ops.tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[ColumnFamilyFunctor].getCanonicalName)
    ops.tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    ops.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }
}

object GeoHashWritableIndex {
  val INDEX_FLAG = "0"
  val DATA_FLAG = "1"

  val INDEX_CHECK = s"~$INDEX_FLAG~"
  val DATA_CHECK = s"~$DATA_FLAG~"
}