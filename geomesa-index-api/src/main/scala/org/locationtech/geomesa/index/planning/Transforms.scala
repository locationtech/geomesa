/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import org.geotools.process.vector.TransformProcess
import org.geotools.process.vector.TransformProcess.Definition
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Convenience methods for working with relational query projections (aka transforms)
  */
object Transforms {

  import scala.collection.JavaConverters._

  /**
    * Does the simple feature type contain the fields required to evaluate the transform and filter
    *
    * @param sft simple feature type
    * @param transforms transform definitions
    * @param filter filter
    * @return
    */
  def supports(sft: SimpleFeatureType, transforms: Seq[Definition], filter: Option[Filter]): Boolean = {
    val names = filter.toSeq.flatMap(FilterHelper.propertyNames(_, sft)) ++
      transforms.flatMap(t => FilterHelper.propertyNames(t.expression, sft))
    names.forall(sft.indexOf(_) != -1)
  }

  /**
    * Parses transform string into definitions
    *
    * @param transforms transform definitions
    * @return
    */
  def definitions(transforms: String): Seq[Definition] = TransformProcess.toDefinition(transforms).asScala
}
