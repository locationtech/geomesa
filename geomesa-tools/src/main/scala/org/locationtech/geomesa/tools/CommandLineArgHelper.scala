/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import com.beust.jcommander.ParameterException
import com.typesafe.config.Config
import org.locationtech.geomesa.convert.ConverterConfigParser
import org.locationtech.geomesa.utils.geotools.SftArgParser
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Wrapper for SFT and Config parsing that throws ParameterExceptions for JCommander
 */
object CommandLineArgHelper {

  /**
   * @throws ParameterException if the SFT cannot be parsed
   * @return the SFT parsed from the Args
   */
  @throws[ParameterException]
  def getSft(specArg: String, featureName: String = null): SimpleFeatureType =
    SftArgParser.getSft(specArg, featureName).getOrElse {
      throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
    }

  /**
   * @throws ParameterException if the config cannot be parsed
   * @return the converter config parsed from the args
   */
  @throws[ParameterException]
  def getConfig(configArg: String): Config =
    ConverterConfigParser.getConfig(configArg)
      .getOrElse(throw new ParameterException(s"Unable to parse Converter config from argument $configArg"))
}
