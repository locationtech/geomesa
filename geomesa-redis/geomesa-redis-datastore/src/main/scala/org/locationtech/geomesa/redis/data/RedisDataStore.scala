/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import org.geotools.data.Query
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils._
import org.locationtech.geomesa.redis.data.RedisDataStore.RedisDataStoreConfig
import org.locationtech.geomesa.redis.data.index.{RedisIndexAdapter, RedisQueryPlan}
import org.locationtech.geomesa.redis.data.util.{RedisBackedMetadata, RedisGeoMesaStats, RedisLocking}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType
import redis.clients.jedis.JedisPool

/**
  * Data store backed by Redis. Uses Redis SortedSets for range scanning
  *
  * @param connection connection pool
  * @param config datastore configuration
  */
class RedisDataStore(val connection: JedisPool, override val config: RedisDataStoreConfig)
    extends GeoMesaDataStore[RedisDataStore](config) with RedisLocking {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  override val metadata: GeoMesaMetadata[String] =
    new RedisBackedMetadata(connection, config.catalog, MetadataStringSerializer)

  override val adapter: RedisIndexAdapter = new RedisIndexAdapter(this)

  override val stats: GeoMesaStats = RedisGeoMesaStats(this)

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaCreate(sft: SimpleFeatureType): Unit = {
    if (sft.getVisibilityLevel == VisibilityLevel.Attribute) {
      throw new IllegalArgumentException("Attribute level visibility is not supported in this store")
    }

    sft.getAttributeDescriptors.asScala.foreach { descriptor =>
      if (descriptor.getColumnGroups().nonEmpty) {
        throw new IllegalArgumentException("Column groups are not supported in this store")
      }
    }

    // disable shards
    sft.setZShards(0)
    sft.setIdShards(0)
    sft.setAttributeShards(0)

    super.preSchemaCreate(sft)
  }

  override def getQueryPlan(query: Query, index: Option[String], explainer: Explainer): Seq[RedisQueryPlan] =
    super.getQueryPlan(query, index, explainer).asInstanceOf[Seq[RedisQueryPlan]]

  override def dispose(): Unit = {
    CloseWithLogging(connection)
    super.dispose()
  }
}

object RedisDataStore {

  case class RedisDataStoreConfig(
      catalog: String,
      generateStats: Boolean,
      audit: Option[(AuditWriter, AuditProvider, String)],
      pipeline: Boolean,
      queryThreads: Int,
      queryTimeout: Option[Long],
      looseBBox: Boolean,
      caching: Boolean,
      authProvider: AuthorizationsProvider,
      namespace: Option[String]) extends GeoMesaDataStoreConfig
}
