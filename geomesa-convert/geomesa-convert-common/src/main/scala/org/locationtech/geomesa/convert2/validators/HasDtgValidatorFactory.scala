/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.opengis.feature.simple.SimpleFeatureType

class HasDtgValidatorFactory extends SimpleFeatureValidatorFactory {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = HasDtgValidatorFactory.Name

  override def apply(
      sft: SimpleFeatureType,
      metrics: ConverterMetrics,
      config: Option[String]): SimpleFeatureValidator = {
    val i = sft.getDtgIndex.getOrElse(-1)
    if (i == -1) { NoValidator } else { new NullValidator(i, Errors.DateNull, metrics.counter("validators.dtg.null")) }
  }
}

object HasDtgValidatorFactory {
  val Name = "has-dtg"
}
