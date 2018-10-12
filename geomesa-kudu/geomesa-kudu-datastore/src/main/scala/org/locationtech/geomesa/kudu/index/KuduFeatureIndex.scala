/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._
import org.apache.kudu.{ColumnSchema, Schema}
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace.ScanRange
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.kudu._
import org.locationtech.geomesa.kudu.data.KuduQueryPlan.{EmptyPlan, ScanPlan}
import org.locationtech.geomesa.kudu.data._
import org.locationtech.geomesa.kudu.index.z2.{KuduXZ2Index, KuduZ2Index}
import org.locationtech.geomesa.kudu.index.z3.{KuduXZ3Index, KuduZ3Index}
import org.locationtech.geomesa.kudu.result.KuduResultAdapter
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.VisibilityAdapter
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduFilter
import org.locationtech.geomesa.kudu.schema.{KuduColumnAdapter, KuduSimpleFeatureSchema}
import org.locationtech.geomesa.kudu.utils.RichKuduClient.SessionHolder
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.KVPairParser
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object KuduFeatureIndex extends KuduIndexManagerType {

  val KuduSplitterOptions = "geomesa.kudu.splitters"

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[KuduFeatureIndex[_, _]] =
    Seq(KuduZ3Index, KuduXZ3Index, KuduZ2Index, KuduXZ2Index, KuduIdIndex, KuduAttributeIndex)

  override val CurrentIndices: Seq[KuduFeatureIndex[_, _]] =
    Seq(KuduZ3Index, KuduXZ3Index, KuduZ2Index, KuduXZ2Index, KuduIdIndex, KuduAttributeIndex)

  def splitters(sft: SimpleFeatureType): Map[String, String] =
    Option(sft.getUserData.get(KuduSplitterOptions).asInstanceOf[String]).map(KVPairParser.parse).getOrElse(Map.empty)
}

trait KuduFeatureIndex[T, U] extends KuduFeatureIndexType with LazyLogging {

  import scala.collection.JavaConverters._

  private val tableSchemas = new ConcurrentHashMap[String, Schema]()

  /**
    * Columns that are part of the primary key, used for range planning
    *
    * @return
    */
  protected def keyColumns: Seq[KuduColumnAdapter[_]]

  /**
    * Primary key space used by this index
    *
    * @return
    */
  protected def keySpace: IndexKeySpace[T, U]

  /**
    * Create initial partitions based on table splitting config
    *
    * @param sft simple feature type
    * @param schema table schema
    * @param config table splitting config
    * @param options options to modify with partitions
    */
  protected def configurePartitions(sft: SimpleFeatureType,
                                    schema: Schema,
                                    config: Map[String, String],
                                    options: CreateTableOptions): Unit

  /**
    * Turns a scan range into a kudu range
    *
    * @param sft simple feature type
    * @param schema table schema
    * @param range scan range
    * @return
    */
  protected def toRowRanges(sft: SimpleFeatureType,
                            schema: Schema,
                            range: ScanRange[U]): (Option[PartialRow], Option[PartialRow])

  /**
    * Creates key values for insert
    *
    * @param toIndexKey index key creation
    * @param kf feature to insert
    * @return
    */
  protected def createKeyValues(toIndexKey: SimpleFeature => Seq[U])(kf: KuduFeature): Seq[Seq[KuduValue[_]]]

  /**
    * Creates a new ranges partition that will cover the time period. Only implemented by indices
    * with a leading time period, as otherwise we have to partition up front. Kudu only supports
    * adding new range partitions that don't overlap any existing partitions - you can't modify or
    * split existing partitions
    *
    * @param sft simple feature type
    * @param table kudu table
    * @param splitters table splitting config
    * @param bin time period being covered (e.g. week)
    * @return
    */
  protected def createPartition(sft: SimpleFeatureType,
                                table: KuduTable,
                                splitters: Map[String, String],
                                bin: Short): Option[Partitioning] = None

  override def supports(sft: SimpleFeatureType): Boolean = keySpace.supports(sft)

  override def configure(sft: SimpleFeatureType, ds: KuduDataStore, partition: Option[String]): String = {
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.TABLE_SPLITTER_OPTS

    val table = super.configure(sft, ds, partition)

    if (!ds.client.tableExists(table)) {
      val schema = tableSchema(sft)
      val options = new CreateTableOptions()
      val splitters =
        Option(sft.getUserData.get(TABLE_SPLITTER_OPTS).asInstanceOf[String]).map(KVPairParser.parse).getOrElse(Map.empty)

      configurePartitions(sft, schema, splitters, options)

      // default replication is 3, but for small clusters that haven't been
      // updated using the default will throw an exception
      val servers = ds.client.listTabletServers.getTabletServersCount
      if (servers < 3) {
        options.setNumReplicas(servers)
      }

      ds.client.createTable(table, schema, options)
    }

    table
  }

  override def writer(sft: SimpleFeatureType, ds: KuduDataStore): KuduFeature => Seq[WriteOperation] = {
    // note: table partitioning is disabled in the data store
    val table = ds.client.openTable(getTableNames(sft, ds, None).head)
    val splitters = KuduFeatureIndex.splitters(sft)
    val toIndexKey = keySpace.toIndexKey(sft)
    val schema = KuduSimpleFeatureSchema(sft)
    createInsert(sft, table, schema, splitters, toIndexKey)
  }

  override def remover(sft: SimpleFeatureType, ds: KuduDataStore): KuduFeature => Seq[WriteOperation] = {
    // note: table partitioning is disabled in the data store
    val table = ds.client.openTable(getTableNames(sft, ds, None).head)
    val toIndexKey = keySpace.toIndexKey(sft, lenient = true)
    createDelete(sft, table, toIndexKey)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String =
    throw new NotImplementedError("Kudu does not support binary rows")

  override def getSplits(sft: SimpleFeatureType, partition: Option[String]): Seq[Array[Byte]] =
    throw new NotImplementedError("Kudu does not support binary splits")

  override def removeAll(sft: SimpleFeatureType, ds: KuduDataStore): Unit = {
    import org.locationtech.geomesa.kudu.utils.RichKuduClient.RichScanner

    getTableNames(sft, ds, None).par.foreach { name =>
      val table = ds.client.openTable(name)
      val builder = ds.client.newScannerBuilder(table)
      builder.setProjectedColumnNames(keyColumns.flatMap(_.columns.map(_.getName)).asJava)

      WithClose(SessionHolder(ds.client.newSession()), builder.build().iterator) { case (holder, iterator) =>
        holder.session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
        iterator.foreach { row =>
          val delete = table.newDelete()
          keyColumns.foreach(_.transfer(row, delete.getRow))
          holder.session.apply(delete)
        }
      }
    }
  }

  override def delete(sft: SimpleFeatureType, ds: KuduDataStore, partition: Option[String]): Unit = {
    getTableNames(sft, ds, partition).par.foreach { table =>
      if (ds.client.tableExists(table)) {
        ds.client.deleteTable(table)
      }
    }

    // deletes the metadata
    super.delete(sft, ds, partition)
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: KuduDataStore,
                            filter: KuduFilterStrategyType,
                            hints: Hints,
                            explain: Explainer): KuduQueryPlanType = {
    val indexValues = filter.primary.map(keySpace.getIndexValues(sft, _, explain))

    val ranges: Iterator[(Option[PartialRow], Option[PartialRow])] = indexValues match {
      case None =>
        // check that full table scans are allowed
        QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
        filter.secondary.foreach { f =>
          logger.debug(s"Running full table scan on $name index for schema ${sft.getTypeName} " +
              s"with filter ${filterToString(f)}")
        }
        Iterator.single((None, None))

      case Some(values) =>
        val schema = tableSchema(sft)
        // TODO add per-datastore (feature type?) config for e.g. geomesa.scan.ranges.target
        // kudu operates better with lower range numbers compared to e.g. accumulo
        keySpace.getRanges(values).map(toRowRanges(sft, schema, _))
    }

    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      val tables = getTablesForQuery(sft, ds, filter.filter)
      val schema = KuduSimpleFeatureSchema(sft)

      val fullFilter =
        if (keySpace.useFullFilter(indexValues, Some(ds.config), hints)) { filter.filter } else { filter.secondary }

      val auths = ds.config.authProvider.getAuthorizations.asScala.map(_.getBytes(StandardCharsets.UTF_8))

      // create push-down predicates and remove from the ecql where possible
      val KuduFilter(predicates, ecql) = fullFilter.map(schema.predicate).getOrElse(KuduFilter(Seq.empty, None))

      val adapter = KuduResultAdapter(sft, auths, ecql, hints)

      ScanPlan(filter, tables, ranges.toSeq, predicates, ecql, adapter, ds.config.queryThreads)
    }
  }

  /**
    * Gets a cached table schema for the simple feature type, which includes all the primary key columns
    * and the feature type columns
    *
    * @param sft simple feature type
    * @return
    */
  protected def tableSchema(sft: SimpleFeatureType): Schema = {
    val key = CacheKeyGenerator.cacheKey(sft)
    var schema = tableSchemas.get(key)
    if (schema == null) {
      val cols = new java.util.ArrayList[ColumnSchema]()
      keyColumns.flatMap(_.columns).foreach(cols.add)
      cols.add(VisibilityAdapter.column)
      KuduSimpleFeatureSchema(sft).writeSchema.foreach(cols.add)
      schema = new Schema(cols)
      tableSchemas.put(key, schema)
    }
    schema
  }

  private def createInsert(sft: SimpleFeatureType,
                           table: KuduTable,
                           schema: KuduSimpleFeatureSchema,
                           splitters: Map[String, String],
                           toIndexKey: SimpleFeature => Seq[U])
                          (kf: KuduFeature): Seq[WriteOperation] = {
    val featureValues = schema.serialize(kf.feature)
    val vis = SecurityUtils.getVisibility(kf.feature)
    val partitioning = () => createPartition(sft, table, splitters, kf.bin)
    createKeyValues(toIndexKey)(kf).map { key =>
      val upsert = table.newUpsert()
      val row = upsert.getRow
      key.foreach(_.writeToRow(row))
      featureValues.foreach(_.writeToRow(row))
      VisibilityAdapter.writeToRow(row, vis)
      WriteOperation(upsert, s"$identifier.${kf.bin}", partitioning)
    }
  }

  private def createDelete(sft: SimpleFeatureType,
                           table: KuduTable,
                           toIndexKey: SimpleFeature => Seq[U])
                          (kf: KuduFeature): Seq[WriteOperation] = {
    createKeyValues(toIndexKey)(kf).map { key =>
      val delete = table.newDelete()
      val row = delete.getRow
      key.foreach(_.writeToRow(row))
      WriteOperation(delete, "", null) // note: we don't deal with partitions on deletes
    }
  }
}
