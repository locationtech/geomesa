/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import com.github.benmanes.caffeine.cache.LoadingCache
import org.locationtech.geomesa.index.planning.InMemoryQueryRunner
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.kafka.data.KafkaCacheLoader
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class KafkaQueryRunner(caches: LoadingCache[String, KafkaCacheLoader],
                       stats: GeoMesaStats,
                       authProvider: Option[AuthorizationsProvider])
    extends InMemoryQueryRunner(stats, authProvider) {

  override protected def name: String = "Kafka"

  override protected def features(sft: SimpleFeatureType, filter: Option[Filter]): CloseableIterator[SimpleFeature] =
    CloseableIterator(caches.get(sft.getTypeName).cache.query(filter.getOrElse(Filter.INCLUDE)))
}
