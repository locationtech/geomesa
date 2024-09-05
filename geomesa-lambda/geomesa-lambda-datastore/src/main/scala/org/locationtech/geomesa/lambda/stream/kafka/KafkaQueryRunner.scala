/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.{Filter, Id}
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.ReadableFeatureCache
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.CloseableIterator

class KafkaQueryRunner(
    features: ReadableFeatureCache,
    authProvider: Option[AuthorizationsProvider],
    override protected val interceptors: QueryInterceptorFactory
  ) extends LocalQueryRunner(authProvider) {

  override protected val name: String = "Kafka lambda"

  override protected def features(sft: SimpleFeatureType, filter: Option[Filter]): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConverters._
    val iter = filter match {
      case Some(f: Id) => f.getIDs.iterator.asScala.map(i => features.get(i.toString)).filter(_ != null)
      case Some(f)     => features.all().filter(f.evaluate)
      case None        => features.all()
    }
    CloseableIterator(iter)
  }
}
