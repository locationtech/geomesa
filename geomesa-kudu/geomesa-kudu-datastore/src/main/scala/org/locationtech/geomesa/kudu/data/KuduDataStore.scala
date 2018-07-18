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
import org.geotools.data.Query
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.stats.{DistributedRunnableStats, GeoMesaStats}
import org.locationtech.geomesa.index.utils.{Explainer, LocalLocking}
import org.locationtech.geomesa.kudu._
import org.locationtech.geomesa.kudu.data.KuduDataStoreFactory.KuduDataStoreConfig
import org.locationtech.geomesa.kudu.data.KuduFeatureWriter.{KuduAppendFeatureWriter, KuduModifyFeatureWriter}
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex
import org.locationtech.geomesa.kudu.index.z3.{KuduXZ3Index, KuduZ3Index}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class KuduDataStore(val client: KuduClient, override val config: KuduDataStoreConfig)
    extends KuduDataStoreType(config) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] =
    new KuduBackedMetadata(client, config.catalog, MetadataStringSerializer)

  override val manager: KuduIndexManagerType = KuduFeatureIndex

  override val stats: GeoMesaStats = new DistributedRunnableStats(this)

  override protected def createQueryPlanner(): KuduQueryPlanner = new KuduQueryPlanner(this)

  override protected def createFeatureCollection(query: Query, source: GeoMesaFeatureSource): KuduFeatureCollection =
    new KuduFeatureCollection(source, query)

  override protected def createFeatureWriterAppend(sft: SimpleFeatureType,
                                                   indices: Option[Seq[KuduFeatureIndexType]]): KuduFeatureWriterType = {
    val session = client.newSession()
    // increase the number of mutations that we can buffer
    session.setMutationBufferSpace(KuduSystemProperties.MutationBufferSpace.toInt.get)
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

    new KuduAppendFeatureWriter(sft, this, indices, session)
  }

  override protected def createFeatureWriterModify(sft: SimpleFeatureType,
                                                   indices: Option[Seq[KuduFeatureIndexType]],
                                                   filter: Filter): KuduFeatureWriterType = {
    val session = client.newSession()
    // increase the number of mutations that we can buffer
    session.setMutationBufferSpace(KuduSystemProperties.MutationBufferSpace.toInt.get)
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

    new KuduModifyFeatureWriter(sft, this, indices, filter, session)
  }

  override protected def validateNewSchema(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // table sharing is always false
    sft.setTableSharing(false)
    // set table splitter options with a key that will be persisted, as we use it after table creation
    Option(sft.getTableSplitterOptions).foreach(sft.getUserData.put(KuduFeatureIndex.KuduSplitterOptions, _))
    // if not specified, only enable the z3 index and let kudu handle other predicates
    if (!sft.getUserData.containsKey(SimpleFeatureTypes.Configs.ENABLED_INDICES)) {
      Seq(KuduZ3Index, KuduXZ3Index).find(_.supports(sft)).foreach { index =>
        sft.getUserData.put(SimpleFeatureTypes.Configs.ENABLED_INDICES, index.name)
      }
    }

    super.validateNewSchema(sft)
  }

  override def delete(): Unit = {
    val tables = getTypeNames.map(getSchema).flatMap { sft =>
      manager.indices(sft).map(_.getTableName(sft.getTypeName, this))
    }
    (tables.distinct :+ config.catalog).par.foreach { table =>
      client.deleteTable(table)
    }
  }

  override def getQueryPlan(query: Query,
                            index: Option[KuduFeatureIndexType],
                            explainer: Explainer): Seq[KuduQueryPlan] = {
    super.getQueryPlan(query, index, explainer).asInstanceOf[Seq[KuduQueryPlan]]
  }

  override def dispose(): Unit = {
    try {
      super.dispose()
    } finally {
      client.close()
    }
  }
}
