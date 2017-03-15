/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.features.arrow

import java.io.{Closeable, InputStream}

import org.opengis.feature.simple.SimpleFeature

class ArrowSimpleFeatureReader(is: InputStream) extends Closeable {

  def read(decodeDictionaries: Boolean = true): Iterator[SimpleFeature] = {
    null
  }

  override def close(): Unit = {

  }
}
