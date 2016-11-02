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
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.hbase.{HBaseDataStoreType, HBaseFeatureWriterType, HBaseIndexManagerType}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, NoopStats}
import org.locationtech.geomesa.index.utils.{GeoMesaMetadata, LocalLocking}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class HBaseDataStore(val connection: Connection, config: HBaseDataStoreConfig)
    extends HBaseDataStoreType(config) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] =
    new HBaseBackedMetadata(connection, TableName.valueOf(config.catalog), new HBaseStringSerializer)

  override def manager: HBaseIndexManagerType = HBaseFeatureIndex

  override def stats: GeoMesaStats = NoopStats

  override def createFeatureWriterAppend(sft: SimpleFeatureType): HBaseFeatureWriterType =
    new HBaseAppendFeatureWriter(sft, this)

  override def createFeatureWriterModify(sft: SimpleFeatureType, filter: Filter): HBaseFeatureWriterType =
    new HBaseModifyFeatureWriter(sft, this, filter)

  override def createSchema(sft: SimpleFeatureType): Unit = {
    // TODO GEOMESA-1322 support tilde in feature name
    if (sft.getTypeName.contains("~")) {
      throw new IllegalArgumentException("AccumuloDataStore does not currently support '~' in feature type names")
    }
    super.createSchema(sft)
  }

  override def dispose(): Unit = {
    connection.close()
    super.dispose()
  }
}
