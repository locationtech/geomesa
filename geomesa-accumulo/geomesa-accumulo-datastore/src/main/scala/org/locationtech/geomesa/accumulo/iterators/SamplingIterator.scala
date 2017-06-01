/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Map => jMap}

import org.apache.accumulo.core.client.IteratorSetting
import org.geotools.factory.Hints
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.index.utils.FeatureSampler

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

  def configure(is: IteratorSetting, sft: SimpleFeatureType, hints: Hints): Unit = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    hints.getSampling.foreach(configure(is, sft, _))
  }

  def configure(is: IteratorSetting, sft: SimpleFeatureType, sampling: (Float, Option[String])): Unit = {
    val (percent, by) = sampling
    require(percent > 0 && percent < 1f, "Sampling must be a percentage between (0, 1)")
    val nth = (1 / percent.toFloat).toInt
    if (nth > 1) {
      is.addOption(SAMPLE_OPT, nth.toString)
      by.map(sft.indexOf).filter(_ != -1).foreach(i => is.addOption(SAMPLE_BY_OPT, i.toString))
    }
  }
}
