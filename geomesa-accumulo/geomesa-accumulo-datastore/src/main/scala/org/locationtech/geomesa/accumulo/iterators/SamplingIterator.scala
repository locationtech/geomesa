/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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
    sampling.map(SamplingIterator.sample(_, sampleBy))
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

  /**
    * Returns a sampling function that will indicate if a feature should be kept or discarded
    *
    * @param nth will keep every nth feature
    * @param field field to use for threading of samples
    * @return sampling function
    */
  def sample(nth: Int, field: Option[Int]): (SimpleFeature) => Boolean = {
    field match {
      case None =>
        var i = 1
        (_) => if (i == 1) { i += 1; true } else if (i < nth) { i += 1; false } else { i = 1; false }
      case Some(f) =>
        val i = scala.collection.mutable.HashMap.empty[String, Int].withDefaultValue(1)
        (sf) => {
          val value = sf.getAttribute(f)
          val key = if (value == null) "" else value.toString
          val count = i(key)
          if (count < nth) { i(key) = count + 1; count == 1 } else { i(key) = 1; false }
        }
    }
  }
}
