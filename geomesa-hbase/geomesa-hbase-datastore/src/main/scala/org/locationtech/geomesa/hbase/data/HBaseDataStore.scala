/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.security.visibility.Authorizations
import org.geotools.data.Query
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, UnoptimizedRunnableStats}
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer, LocalLocking}
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class HBaseDataStore(val connection: Connection, override val config: HBaseDataStoreConfig)
    extends HBaseDataStoreType(config) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] =
    new HBaseBackedMetadata(connection, TableName.valueOf(config.catalog), MetadataStringSerializer)

  override def manager: HBaseIndexManagerType = HBaseFeatureIndex

  override def stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createFeatureWriterAppend(sft: SimpleFeatureType,
                                         indices: Option[Seq[HBaseFeatureIndexType]]): HBaseFeatureWriterType =
    new HBaseAppendFeatureWriter(sft, this, indices)

  override def createFeatureWriterModify(sft: SimpleFeatureType,
                                         indices: Option[Seq[HBaseFeatureIndexType]],
                                         filter: Filter): HBaseFeatureWriterType =
    new HBaseModifyFeatureWriter(sft, this, indices, filter)

  override def createSchema(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // we are only allowed to set splits at table creation
    // disable table sharing to allow for decent pre-splitting
    sft.setTableSharing(false)
    super.createSchema(sft)
  }

  override def delete(): Unit = {
    val tables = getTypeNames.map(getSchema).flatMap { sft =>
      manager.indices(sft, IndexMode.Any).map(_.getTableName(sft.getTypeName, this))
    }
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

  override def dispose(): Unit = {
    super.dispose()
  }

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
}

object HBaseDataStore {
  import scala.collection.JavaConverters._
  val EmptyAuths: java.util.List[String] = List("").asJava
}