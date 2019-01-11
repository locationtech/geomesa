/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import org.apache.kudu.client.KuduClient
import org.geotools.data.Query
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.stats.{DistributedRunnableStats, GeoMesaStats}
import org.locationtech.geomesa.index.utils.{Explainer, LocalLocking}
import org.locationtech.geomesa.kudu._
import org.locationtech.geomesa.kudu.data.KuduDataStoreFactory.KuduDataStoreConfig
import org.locationtech.geomesa.kudu.data.KuduFeatureWriter.KuduFeatureWriterFactory
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex
import org.locationtech.geomesa.kudu.index.z3.{KuduXZ3Index, KuduZ3Index}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.opengis.feature.simple.SimpleFeatureType

class KuduDataStore(val client: KuduClient, override val config: KuduDataStoreConfig)
    extends KuduDataStoreType(config) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] =
    new KuduBackedMetadata(client, config.catalog, MetadataStringSerializer)

  override val manager: KuduIndexManagerType = KuduFeatureIndex

  override val stats: GeoMesaStats = new DistributedRunnableStats(this)

  override protected val featureWriterFactory: KuduFeatureWriterFactory = new KuduFeatureWriterFactory(this)

  override protected def createQueryPlanner(): KuduQueryPlanner = new KuduQueryPlanner(this)

  override protected def createFeatureCollection(query: Query, source: GeoMesaFeatureSource): KuduFeatureCollection =
    new KuduFeatureCollection(source, query)

  override protected def validateNewSchema(sft: SimpleFeatureType): Unit = {
    import Configs.TABLE_SPLITTER_OPTS
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // table sharing is always false
    sft.setTableSharing(false)
    // set table splitter options with a key that will be persisted, as we use it after table creation
    Option(sft.getUserData.get(TABLE_SPLITTER_OPTS)).foreach(sft.getUserData.put(KuduFeatureIndex.KuduSplitterOptions, _))

    // if not specified, only enable the z3 index and let kudu handle other predicates
    if (!sft.getUserData.containsKey(SimpleFeatureTypes.Configs.ENABLED_INDICES)) {
      Seq(KuduZ3Index, KuduXZ3Index).find(_.supports(sft)).foreach { index =>
        sft.getUserData.put(SimpleFeatureTypes.Configs.ENABLED_INDICES, index.name)
      }
    }
    // suppress table partitioning, as it doesn't fit well with the kudu api, and kudu supports native partitioning
    if (TablePartition.partitioned(sft)) {
      logger.warn("Table partitioning is not supported - disabling")
      sft.getUserData.remove(SimpleFeatureTypes.Configs.TABLE_PARTITIONING)
    }

    super.validateNewSchema(sft)
  }

  override def delete(): Unit = {
    val tables = getTypeNames.map(getSchema).flatMap { sft =>
      manager.indices(sft).flatMap(_.getTableNames(sft, this, None))
    }
    (tables.distinct :+ config.catalog).par.foreach(client.deleteTable)
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
