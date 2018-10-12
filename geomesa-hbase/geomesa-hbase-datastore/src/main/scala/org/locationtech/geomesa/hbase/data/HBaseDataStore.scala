/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.util.Collections

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.security.visibility.Authorizations
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseVersionAggregator
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.hbase.data.HBaseFeatureWriter.HBaseFeatureWriterFactory
import org.locationtech.geomesa.hbase.index.{HBaseColumnGroups, HBaseFeatureIndex}
import org.locationtech.geomesa.index.geotools.{GeoMesaFeatureCollection, GeoMesaFeatureSource}
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.index.stats.{DistributedRunnableStats, GeoMesaStats, UnoptimizedRunnableStats}
import org.locationtech.geomesa.index.utils._
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

class HBaseDataStore(val connection: Connection, override val config: HBaseDataStoreConfig)
    extends HBaseDataStoreType(config) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] =
    new HBaseBackedMetadata(connection, TableName.valueOf(config.catalog), MetadataStringSerializer)

  override def manager: HBaseIndexManagerType = HBaseFeatureIndex

  override val stats: GeoMesaStats =
    if (config.remoteFilter) { new DistributedRunnableStats(this) } else { new UnoptimizedRunnableStats(this) }

  override protected val featureWriterFactory: HBaseFeatureWriterFactory = new HBaseFeatureWriterFactory(this)

  @throws(classOf[IllegalArgumentException])
  override protected def validateNewSchema(sft: SimpleFeatureType): Unit = {
    super.validateNewSchema(sft)
    HBaseColumnGroups.validate(sft)
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // we are only allowed to set splits at table creation
    // disable table sharing to allow for decent pre-splitting
    sft.setTableSharing(false)
    super.createSchema(sft)
  }

  override def delete(): Unit = {
    val tables = getTypeNames.flatMap(getAllIndexTableNames)
    val admin = connection.getAdmin
    try {
      (tables.distinct :+ config.catalog).map(TableName.valueOf).par.foreach { table =>
        admin.disableTable(table)
        admin.deleteTable(table)
      }
    } finally {
      admin.close()
    }
  }

  override def getQueryPlan(query: Query,
                            index: Option[HBaseFeatureIndexType] = None,
                            explainer: Explainer = new ExplainLogging): Seq[HBaseQueryPlan] = {
    super.getQueryPlan(query, index, explainer).asInstanceOf[Seq[HBaseQueryPlan]]
  }

  override protected def createFeatureCollection(query: Query, source: GeoMesaFeatureSource): GeoMesaFeatureCollection =
    new HBaseFeatureCollection(source, query)

  def applySecurity(query: org.apache.hadoop.hbase.client.Query): Unit =
    authOpt.foreach(query.setAuthorizations)

  def applySecurity(queries: Iterable[org.apache.hadoop.hbase.client.Query]): Unit =
    authOpt.foreach { a => queries.foreach(_.setAuthorizations(a))}

  private[this] def authOpt: Option[Authorizations] =
    config.authProvider.map(_.getAuthorizations).map { auths =>
      // HBase seems to treat and empty collection as no auths
      // which forces it to default to the user's full set of auths
      if (auths.isEmpty) { HBaseDataStore.EmptyAuths }
      else { auths }
    }.map(new Authorizations(_))

  override protected def createQueryPlanner(): QueryPlanner[HBaseDataStore, HBaseFeature, Mutation] =
    new HBaseQueryPlanner(this)

  override protected def loadIteratorVersions: Set[String] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    // just check the first table available
    val versions = getTypeNames.iterator.map(getSchema).flatMap { sft =>
      manager.indices(sft).iterator.flatMap { index =>
        index.getTableNames(sft, this, None).flatMap { table =>
          try {
            val name = TableName.valueOf(table)
            if (connection.getAdmin.tableExists(name)) {
              val options = HBaseVersionAggregator.configure(sft, index)
              WithClose(connection.getTable(name)) { t =>
                WithClose(GeoMesaCoprocessor.execute(t, new Scan().setFilter(new FilterList()), options)) { bytes =>
                  bytes.map(_.toStringUtf8).toList.iterator // force evaluation of the iterator before closing it
                }
              }
            } else {
              Iterator.empty
            }
          } catch {
            case NonFatal(_) => Iterator.empty
          }
        }
      }
    }
    versions.headOption.toSet
  }
}

class HBaseQueryPlanner(ds: HBaseDataStore) extends HBaseQueryPlannerType(ds) {
  // This function calculates the SimpleFeatureType of the returned SFs.
  override protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      StatsScan.StatsSft
    } else {
      super.getReturnSft(sft, hints)
    }
  }
}

object HBaseDataStore {
  val EmptyAuths: java.util.List[String] = Collections.singletonList("")
}
