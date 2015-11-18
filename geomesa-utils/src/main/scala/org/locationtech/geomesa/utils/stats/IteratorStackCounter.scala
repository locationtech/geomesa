/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/


package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

case class IteratorStackCounter extends Stat {
  var count: Long = 1

  override def observe(sf: SimpleFeature): Unit = { }

  override def toJson(): String = s"iteratorStackCount:$count"

  override def add(other: Stat): Stat = {
    other match {
      case o: IteratorStackCounter => count += o.count
    }
    this
  }
}
