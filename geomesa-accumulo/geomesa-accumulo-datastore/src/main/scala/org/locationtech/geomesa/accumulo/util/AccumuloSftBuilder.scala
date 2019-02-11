/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.locationtech.geomesa.index.conf.TableSplitter
import org.locationtech.geomesa.utils.geotools.SftBuilder.{SepEntry, SepPart, encodeMap}
import org.locationtech.geomesa.utils.geotools.{InitBuilder, SimpleFeatureTypes}

@deprecated("AccumuloSchemaBuilder")
class AccumuloSftBuilder extends InitBuilder[AccumuloSftBuilder] {
  def recordSplitter(clazz: String, splitOptions: Map[String,String]): AccumuloSftBuilder = {
    // note that SimpleFeatureTypes requires that splitter and splitter opts be ordered properly
    userData(SimpleFeatureTypes.Configs.TABLE_SPLITTER, clazz)
    if (splitOptions.nonEmpty) {
      userData(SimpleFeatureTypes.Configs.TABLE_SPLITTER_OPTS, encodeMap(splitOptions, SepPart, SepEntry))
    }
    this
  }
  def recordSplitter(clazz: Class[_ <: TableSplitter], splitOptions: Map[String,String]): AccumuloSftBuilder =
    recordSplitter(clazz.getName, splitOptions)
}
