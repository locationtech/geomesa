/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.{Map => jMap}

import org.opengis.feature.simple.SimpleFeature

/**
  * Mixin trait to provide support for sampling features.
  *
  * Current implementation takes every nth feature. As such, sampling
  * percents > 0.5 will not have any effect.
  */
trait SamplingIterator {

  /**
    * Configure a sampling function based on the iterator configuration
    *
    * @param options iterator options
    * @return sampling function, if defined
    */
  def sample(options: jMap[String, String]): Option[(SimpleFeature) => Boolean] = {
    import scala.collection.JavaConverters._
    sample(options.asScala.toMap)
  }

  /**
    * Configure a sampling function based on the iterator configuration
    *
    * @param options iterator options
    * @return sampling function, if defined
    */
  def sample(options: Map[String, String]): Option[(SimpleFeature) => Boolean] = {
    import SamplingIterator.{SAMPLE_BY_OPT, SAMPLE_OPT}
    val sampling = options.get(SAMPLE_OPT).map(_.toInt)
    val sampleBy = options.get(SAMPLE_BY_OPT).map(_.toInt)
    sampling.map(FeatureSampler.sample(_, sampleBy))
  }
}

object SamplingIterator {
  val SAMPLE_OPT    = "sample"
  val SAMPLE_BY_OPT = "sample-by"
}
