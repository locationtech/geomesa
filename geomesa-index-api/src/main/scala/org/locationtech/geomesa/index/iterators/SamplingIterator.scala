/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.util.{Map => jMap}

import org.locationtech.geomesa.index.utils.FeatureSampler
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
  def sample(options: jMap[String, String]): Option[SimpleFeature => Boolean] = {
    import scala.collection.JavaConverters._
    sample(options.asScala.toMap)
  }

  /**
    * Configure a sampling function based on the iterator configuration
    *
    * @param options iterator options
    * @return sampling function, if defined
    */
  def sample(options: Map[String, String]): Option[SimpleFeature => Boolean] = {
    import SamplingIterator.Configuration.{SampleByOpt, SampleOpt}
    val sampling = options.get(SampleOpt).map(_.toInt)
    val sampleBy = options.get(SampleByOpt).map(_.toInt)
    sampling.map(FeatureSampler.sample(_, sampleBy))
  }
}

object SamplingIterator {

  object Configuration {
    val SampleOpt   = "sample"
    val SampleByOpt = "sample-by"
  }

  def configure(sft: SimpleFeatureType, sampling: (Float, Option[String])): Map[String, String] = {
    import Configuration.{SampleByOpt, SampleOpt}
    val (percent, by) = sampling
    require(percent > 0 && percent < 1f, "Sampling must be a percentage between (0, 1)")
    val nth = (1 / percent.toFloat).toInt
    if (nth <= 1) { Map.empty } else {
      val sampleBy = by.map(sft.indexOf).collect {
        case i if i != -1 => SampleByOpt -> i.toString
      }
      sampleBy match {
        case None     => Map(SampleOpt -> nth.toString)
        case Some(kv) => Map(SampleOpt -> nth.toString, kv)
      }
    }
  }
}
