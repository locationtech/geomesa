/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.raster.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.locationtech.geomesa.raster.iterators.BBOXCombiner

object AccumuloRasterBoundsPlanner extends LazyLogging {

  def getBoundsScannerCfg(tableName: String): IteratorSetting = {
    logger.debug(s"Raster Bounds Planner for table: $tableName")
    val cfg = new IteratorSetting(10, "GEOMESA_BBOX_COMBINER", classOf[BBOXCombiner])
    cfg.addOption("all", "true")
    cfg
  }

}
