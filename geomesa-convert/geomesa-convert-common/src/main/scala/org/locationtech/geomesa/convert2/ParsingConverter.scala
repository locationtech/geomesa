/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature

/**
  * Converter that parses out intermediate values from an input stream
  *
  * @tparam T type binding
  */
trait ParsingConverter[T] extends SimpleFeatureConverter {

  /**
    * Convert parsed values into simple features
    *
    * @param values parsed values, from `parse`
    * @param ec evaluation context
    * @return
    */
  def convert(values: CloseableIterator[T], ec: EvaluationContext): CloseableIterator[SimpleFeature]
}
