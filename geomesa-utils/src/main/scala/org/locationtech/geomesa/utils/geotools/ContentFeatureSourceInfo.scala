/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/
package org.locationtech.geomesa.utils.geotools

import org.geotools.data.store.ContentFeatureSource
import org.geotools.data.ResourceInfo


trait ContentFeatureSourceInfo extends ContentFeatureSource {

  abstract override def getInfo : ResourceInfo = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val ri = super.getInfo
    ri.getKeywords.addAll(getSchema.getKeywords)
    ri
  }
}
