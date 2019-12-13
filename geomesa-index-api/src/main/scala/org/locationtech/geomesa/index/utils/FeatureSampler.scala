/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.opengis.feature.simple.SimpleFeature

object FeatureSampler {
  /**
   * Returns a sampling function that will indicate if a feature should be kept or discarded
   *
   * @param nth will keep every nth feature
   * @param field field to use for threading of samples
   * @return sampling function
   */
  def sample(nth: Int, field: Option[Int]): SimpleFeature => Boolean = {
    field match {
      case None =>
        var i = 1
        _ => if (i == 1) { i += 1; true } else if (i < nth) { i += 1; false } else { i = 1; false }
      case Some(f) =>
        val i = scala.collection.mutable.HashMap.empty[String, Int].withDefaultValue(1)
        sf => {
          val value = sf.getAttribute(f)
          val key = if (value == null) "" else value.toString
          val count = i(key)
          if (count < nth) { i(key) = count + 1; count == 1 } else { i(key) = 1; false }
        }
    }
  }
}
