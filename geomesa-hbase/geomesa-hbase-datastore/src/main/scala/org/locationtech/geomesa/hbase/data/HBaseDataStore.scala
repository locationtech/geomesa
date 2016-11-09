/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.index.stats.{GeoMesaStats, UnoptimizedRunnableStats}
import org.locationtech.geomesa.index.utils.{GeoMesaMetadata, LocalLocking}
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class HBaseDataStore(val connection: Connection, config: HBaseDataStoreConfig)
    extends HBaseDataStoreType(config) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] =
    new HBaseBackedMetadata(connection, TableName.valueOf(config.catalog), new HBaseStringSerializer)

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
    // TODO GEOMESA-1322 support tilde in feature name
    if (sft.getTypeName.contains("~")) {
      throw new IllegalArgumentException("AccumuloDataStore does not currently support '~' in feature type names")
    }
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

  override def dispose(): Unit = {
    connection.close()
    super.dispose()
  }
}
