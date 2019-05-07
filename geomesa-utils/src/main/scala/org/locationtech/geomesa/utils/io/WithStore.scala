/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import org.geotools.data.{DataStore, DataStoreFinder}

/**
  * Look up a data store and safely dispose of it when done
  */
object WithStore {

  import scala.collection.JavaConverters._

  def apply[T](params: Map[String, AnyRef])(fn: DataStore => T): T = apply(params.asJava)(fn)

  def apply[T](params: java.util.Map[String, AnyRef])(fn: DataStore => T): T = {
    val ds = DataStoreFinder.getDataStore(params)
    try { fn(ds) } finally {
      if (ds != null) {
        ds.dispose()
      }
    }
  }
}
