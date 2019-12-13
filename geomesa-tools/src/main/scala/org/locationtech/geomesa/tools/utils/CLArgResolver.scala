/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import com.beust.jcommander.ParameterException
import com.typesafe.config.Config
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver}
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Wrapper for SFT and Config parsing that throws ParameterExceptions for JCommander
 */
object CLArgResolver {

  /**
   * @throws ParameterException if the SFT cannot be parsed
   * @return the SFT parsed from the Args
   */
  @throws[ParameterException]
  def getSft(specArg: String, featureName: String = null): SimpleFeatureType = {
      SftArgResolver.getArg(SftArgs(specArg, featureName)) match {
        case Right(sft) => sft
        case Left(e)    => throw new ParameterException(e)
      }
    }

  /**
   * @throws ParameterException if the config cannot be parsed
   * @return the converter config parsed from the args
   */
  @throws[ParameterException]
  def getConfig(configArg: String): Config =
    ConverterConfigResolver.getArg(ConfArgs(configArg)) match {
      case Right(config) => config
      case Left(e)       => throw new ParameterException(e)
    }
}
