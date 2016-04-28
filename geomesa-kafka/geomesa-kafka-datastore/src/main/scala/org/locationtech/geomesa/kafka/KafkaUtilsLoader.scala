/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.util.ServiceLoader

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.kafka.common.KafkaUtils

object KafkaUtilsLoader extends LazyLogging {
  lazy val kafkaUtils: KafkaUtils = {
    val kuIter = ServiceLoader.load(classOf[KafkaUtils]).iterator()
    if (kuIter.hasNext) {
      val first = kuIter.next()
      if (kuIter.hasNext) {
        logger.warn(s"Multiple geomesa KafkaUtils found.  Should only have one. Using the first: '$first'")
      }
      first
    } else {
      throw new Exception(s"No geomesa KafkaUtils found! Cannot continue.")
    }
  }
}


