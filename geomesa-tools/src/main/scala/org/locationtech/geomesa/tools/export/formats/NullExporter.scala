/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import org.geotools.data.simple.SimpleFeatureCollection

object NullExporter extends FeatureExporter {

  override def export(features: SimpleFeatureCollection): Option[Long] = {
    import org.locationtech.geomesa.utils.geotools.Conversions.toRichSimpleFeatureIterator
    var count = 0L
    features.features().foreach(_ => count += 1)
    Some(count)
  }

  override def flush(): Unit = {}
  override def close(): Unit = {}
}

