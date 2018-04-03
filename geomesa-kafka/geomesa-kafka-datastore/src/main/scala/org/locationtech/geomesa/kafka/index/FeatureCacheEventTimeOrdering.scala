/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.util.Date

import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.slf4j.LoggerFactory

class FeatureCacheEventTimeOrdering(baseCache: KafkaFeatureCache, attr: Int) extends KafkaFeatureCache {
  private val log = LoggerFactory.getLogger(classOf[FeatureCacheEventTimeOrdering])

  import org.locationtech.geomesa.utils.geotools.Conversions._

  override def put(feature: SimpleFeature): Unit = {
    baseCache.query(feature.getID) match {
      case None => baseCache.put(feature)

      case Some(cur) =>
        val dtg = feature.get[Date](attr)
        val curDate = cur.get[Date](attr)
        if(curDate.before(dtg)) {
          baseCache.put(feature)
        } else {
          log.debug(s"Ignoring out of order attribute for feature ${feature.getID} since $curDate is after $dtg")
        }
    }
  }

  override def remove(id: String): Unit = baseCache.remove(id)

  override def clear(): Unit = baseCache.clear()

  override def size(): Int = baseCache.size()

  override def size(filter: Filter): Int = baseCache.size(filter)

  override def query(id: String): Option[SimpleFeature] = baseCache.query(id)

  override def query(filter: Filter): Iterator[SimpleFeature] = baseCache.query(filter)

  override def cleanUp(): Unit = baseCache.cleanUp()

  override def close(): Unit = baseCache.close()
}
