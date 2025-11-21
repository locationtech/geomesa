/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.geotools.api.feature.simple.SimpleFeature

object FeatureSampler {

  /**
   * Returns a sampling function that will indicate if a feature will be kept or discarded.
   *
   * Since the current implementation rounds heavily, large percentages will not actually sample out any
   * features, so will return None
   *
   * @param percent percent of features to keep, between [0, 1]
   * @param field field to use for threading of sampling
   * @return sampling function
   */
  def sample(percent: Float, field: Option[Int]): Option[SimpleFeature => Boolean] = {
    if (!(percent > 0 && percent < 1f)) {
      throw new IllegalArgumentException(s"Sampling must be a percentage between (0, 1): $percent")
    }
    val nth = math.round(1 / percent)
    if (nth <= 1) { None } else {
      Some(sample(nth, field))
    }
  }

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
