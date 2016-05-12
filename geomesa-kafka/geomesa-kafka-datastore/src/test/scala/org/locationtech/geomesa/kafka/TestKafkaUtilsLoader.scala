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

object TestKafkaUtilsLoader extends LazyLogging {
  lazy val testKafkaUtils: TestKafkaUtils = {
    val tkuIter = ServiceLoader.load(classOf[TestKafkaUtils]).iterator()
    if (tkuIter.hasNext) {
      val first = tkuIter.next()
      if (tkuIter.hasNext) {
        logger.warn(s"Multiple geomesa TestKafkaUtils found.  Should only have one. Using the first: '$first'")
      }
      first
    } else {
      throw new Exception(s"No geomesa TestKafkaUtils found! Cannot continue.")
    }
  }
}
