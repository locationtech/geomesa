/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io.File

import com.beust.jcommander.{Parameter, Parameters}

/**
 * Note: this class is a placeholder for the 'aws' functionality implemented in the 'geomesa-hbase' script, to get it
 * to show up in the JCommander help.
 */
class AWSCommand extends Command {

  override val name = "aws"
  override val params = new AWSCommandParameters
  override def execute(): Unit = {}
}

@Parameters(commandDescription = "Utilities for deploying GeoMesa on AWS EMR")
class AWSCommandParameters {}
