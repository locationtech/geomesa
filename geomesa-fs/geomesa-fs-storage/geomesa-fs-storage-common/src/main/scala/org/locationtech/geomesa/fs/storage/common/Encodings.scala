/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.opengis.feature.simple.SimpleFeatureType

object Encodings {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val EncodingKey = "geomesa.fs.encoding"

  def getEncoding(sft: SimpleFeatureType): Option[String] = sft.userData[String](EncodingKey)

  def setEncoding(sft: SimpleFeatureType, encoding: String): Unit = sft.getUserData.put(EncodingKey, encoding)
}
