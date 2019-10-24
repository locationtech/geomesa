/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.StrictLogging
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api.{WritableFeature, _}
import org.locationtech.geomesa.kudu.data.KuduIndexAdapter.{KuduFeatureWrapper, KuduIndexWriter}
import org.locationtech.geomesa.kudu.data.KuduQueryPlan.{EmptyPlan, ScanPlan}
import org.locationtech.geomesa.kudu.index.KuduColumnMapper
import org.locationtech.geomesa.kudu.result.KuduResultAdapter
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.VisibilityAdapter
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduFilter
import org.locationtech.geomesa.kudu.utils.RichKuduClient.SessionHolder
import org.locationtech.geomesa.kudu.{KuduSystemProperties, Partitioning}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class KuduIndexAdapter(ds: KuduDataStore) extends IndexAdapter[KuduDataStore] {

  import org.locationtech.geomesa.kudu.utils.RichKuduClient.RichScanner

  import scala.collection.JavaConverters._

  override def createTable(
      index: GeoMesaFeatureIndex[_, _],
      partition: Option[String],
      splits: => Seq[Array[Byte]]): Unit = {
    val table = index.configureTableName(partition) // writes table name to metadata
    if (!ds.client.tableExists(table)) {
      val mapper = KuduColumnMapper(index)

      val options = mapper.configurePartitions()

      // default replication is 3, but for small clusters that haven't been
      // updated using the default will throw an exception
      val servers = ds.client.listTabletServers.getTabletServersCount
      if (servers < 3) {
        options.setNumReplicas(servers)
      }

      ds.client.createTable(table, mapper.tableSchema, options)
    }
  }

  override def deleteTables(tables: Seq[String]): Unit = {
    tables.par.foreach { table =>
      if (ds.client.tableExists(table)) {
        ds.client.deleteTable(table)
      }
    }
  }

  override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
    require(prefix.isEmpty, s"Expected None but got prefix range $prefix")

    val indices = ds.getTypeNames.map(ds.getSchema).flatMap(ds.manager.indices(_))

    tables.par.foreach { name =>
      val index = indices.find(_.getTableNames().contains(name)).getOrElse {
        throw new IllegalArgumentException(s"Couldn't find index corresponding to table '$name'")
      }
      val columns = KuduColumnMapper(index).keyColumns
      val table = ds.client.openTable(name)
      val builder = ds.client.newScannerBuilder(table)
      builder.setProjectedColumnNames(columns.flatMap(_.columns.map(_.getName)).asJava)

      WithClose(SessionHolder(ds.client.newSession()), builder.build().iterator) { case (holder, iterator) =>
        holder.session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
        iterator.foreach { row =>
          val delete = table.newDelete()
          columns.foreach(_.transfer(row, delete.getRow))
          holder.session.apply(delete)
        }
      }
    }
  }

  // TODO GEOMESA-2548 add per-datastore (feature type?) config for e.g. geomesa.scan.ranges.target
  // kudu operates better with lower range numbers compared to e.g. accumulo

  override def createQueryPlan(strategy: QueryStrategy): KuduQueryPlan = {
    val QueryStrategy(filter, _, keyRanges, tieredKeyRanges, _, hints, _) = strategy
    val index = filter.index

    val mapper = KuduColumnMapper(index)
    val auths = ds.config.authProvider.getAuthorizations.asScala.map(_.getBytes(StandardCharsets.UTF_8))

    // create push-down predicates and remove from the ecql where possible
    val KuduFilter(predicates, ecql) = strategy.ecql.map(mapper.schema.predicate).getOrElse(KuduFilter(Seq.empty, None))

    val adapter = KuduResultAdapter(index.sft, auths, ecql, hints)
    if (keyRanges.isEmpty) { EmptyPlan(filter, adapter) } else {
      val tables = index.getTablesForQuery(filter.filter)
      val ranges = mapper.toRowRanges(keyRanges, tieredKeyRanges)
      ScanPlan(filter, tables, ranges, predicates, ecql, adapter, ds.config.queryThreads)
    }
  }

  override def createWriter(sft: SimpleFeatureType,
                            indices: Seq[GeoMesaFeatureIndex[_, _]],
                            partition: Option[String]): KuduIndexWriter =
    new KuduIndexWriter(ds, indices, new KuduFeatureWrapper(sft, WritableFeature.wrapper(sft, groups)), partition)
}

object KuduIndexAdapter {

  class KuduIndexWriter(ds: KuduDataStore,
                        indices: Seq[GeoMesaFeatureIndex[_, _]],
                        wrapper: KuduFeatureWrapper,
                        partition: Option[String]) extends IndexWriter(indices, wrapper) with StrictLogging {

    // track partitions written by this instance
    private val partitions = scala.collection.mutable.Set.empty[String]

    private val session = SessionHolder(ds.client.newSession())
    // increase the number of mutations that we can buffer
    session.session.setMutationBufferSpace(KuduSystemProperties.MutationBufferSpace.toInt.get)
    session.session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

    private val mappers = indices.toArray.map { index =>
      val mapper = KuduColumnMapper(index)
      // note: table partitioning is disabled in the data store
      val table = index.getTableNames(partition) match {
        case Seq(t) => ds.client.openTable(t) // should always be writing to a single table here
        case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
      }
      (mapper, table)
    }

    private var i = 0

    override protected def write(feature: WritableFeature, values: Array[RowKeyValue[_]], update: Boolean): Unit = {
      val kf = feature.asInstanceOf[KuduWritableFeature]

      i = 0
      while (i < values.length) {
        val (mapper, table) = mappers(i)
        // if we haven't seen this partition before, try to add it
        // it's hard (impossible?) to inspect existing partitions through the kudu API,
        // so we just add them and suppress duplicate partition errors
        if (partitions.add(s"${mapper.index.identifier}.${kf.bin}")) {
          mapper.createPartition(table, kf.bin).foreach { case Partitioning(name, alter) =>
            try { ds.client.alterTable(name, alter) } catch {
              case e if existingPartitionError(e) =>
                logger.debug(s"Tried to create a partition to table $name but it already exists: ", e)
            }
          }
        }
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            val upsert = table.newUpsert()
            val row = upsert.getRow
            mapper.createKeyValues(kv).foreach(_.writeToRow(row))
            kf.kuduValues.foreach(_.writeToRow(row))
            VisibilityAdapter.writeToRow(row, kf.vis)
            session.session.apply(upsert)

          case mkv: MultiRowKeyValue[_] =>
            mkv.split.foreach { kv =>
              val upsert = table.newUpsert()
              val row = upsert.getRow
              mapper.createKeyValues(kv).foreach(_.writeToRow(row))
              kf.kuduValues.foreach(_.writeToRow(row))
              VisibilityAdapter.writeToRow(row, kf.vis)
              session.session.apply(upsert)
            }
        }
        i += 1
      }
    }

    override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      while (i < values.length) {
        val (mapper, table) = mappers(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            val delete = table.newDelete()
            val row = delete.getRow
            mapper.createKeyValues(kv).foreach(_.writeToRow(row))
            // note: we don't worry about partitions for removes, as presumably they already exist...
            session.session.apply(delete)

          case mkv: MultiRowKeyValue[_] =>
            mkv.split.foreach { kv =>
              val delete = table.newDelete()
              val row = delete.getRow
              mapper.createKeyValues(kv).foreach(_.writeToRow(row))
              // note: we don't worry about partitions for removes, as presumably they already exist...
              session.session.apply(delete)
            }
        }
        i += 1
      }
    }

    override def flush(): Unit = session.flush()

    override def close(): Unit = session.close()

    private def existingPartitionError(e: Throwable): Boolean = // TODO more robust matching?
      e.getMessage != null && e.getMessage.startsWith("New range partition conflicts with existing range partition")
  }

  class KuduFeatureWrapper(sft: SimpleFeatureType, delegate: FeatureWrapper) extends FeatureWrapper {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    private val dtgIndex = sft.getDtgIndex
    private val toBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    private val schema = KuduSimpleFeatureSchema(sft)

    override def wrap(feature: SimpleFeature): KuduWritableFeature = {
      new KuduWritableFeature(delegate.wrap(feature), schema, dtgIndex, toBin)
    }
  }
}
