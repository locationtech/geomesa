/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, UnoptimizedRunnableStats}
import org.locationtech.geomesa.index.utils.LocalLocking
import org.locationtech.geomesa.kudu._
import org.locationtech.geomesa.kudu.data.KuduDataStoreFactory.KuduDataStoreConfig
import org.locationtech.geomesa.kudu.data.KuduFeatureWriter.{KuduAppendFeatureWriter, KuduModifyFeatureWriter}
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class KuduDataStore(val client: KuduClient, config: KuduDataStoreConfig)
    extends KuduDataStoreType(config) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] =
    new KuduBackedMetadata(client, config.catalog, MetadataStringSerializer)

  override def manager: KuduIndexManagerType = KuduFeatureIndex

  override def stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createFeatureWriterAppend(sft: SimpleFeatureType,
                                         indices: Option[Seq[KuduFeatureIndexType]]): KuduFeatureWriterType = {
    val session = client.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    new KuduAppendFeatureWriter(sft, this, indices, session)
  }

  override def createFeatureWriterModify(sft: SimpleFeatureType,
                                         indices: Option[Seq[KuduFeatureIndexType]],
                                         filter: Filter): KuduFeatureWriterType = {
    val session = client.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    new KuduModifyFeatureWriter(sft, this, indices, filter, session)
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // table sharing is always false
    sft.setTableSharing(false)
    // set table splitter options with a key that will be persisted, as we use it after table creation
    Option(sft.getTableSplitterOptions).foreach(sft.getUserData.put(KuduFeatureIndex.KuduSplitterOptions, _))
    super.createSchema(sft)
  }

  override def delete(): Unit = {
    val tables = getTypeNames.map(getSchema).flatMap { sft =>
      manager.indices(sft).map(_.getTableName(sft.getTypeName, this))
    }

    (tables.distinct :+ config.catalog).par.foreach { table =>
      client.deleteTable(table)
    }
  }

  override def dispose(): Unit = {
    try {
      super.dispose()
    } finally {
      client.close()
    }
  }
}
