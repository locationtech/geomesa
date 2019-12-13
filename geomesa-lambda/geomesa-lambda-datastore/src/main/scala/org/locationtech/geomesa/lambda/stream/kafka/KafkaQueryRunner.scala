/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.ReadableFeatureCache
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, Id}

class KafkaQueryRunner(features: ReadableFeatureCache,
                       stats: GeoMesaStats,
                       authProvider: Option[AuthorizationsProvider],
                       override protected val interceptors: QueryInterceptorFactory)
    extends LocalQueryRunner(stats, authProvider) {

  override protected val name: String = "Kafka lambda"

  override protected def features(sft: SimpleFeatureType, filter: Option[Filter]): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConversions._
    val iter = filter match {
      case Some(f: Id) => f.getIDs.iterator.map(i => features.get(i.toString)).filter(_ != null)
      case Some(f)     => features.all().filter(f.evaluate)
      case None        => features.all()
    }
    CloseableIterator(iter)
  }
}
