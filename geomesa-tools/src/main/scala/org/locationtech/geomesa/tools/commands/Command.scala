/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.{AccumuloProperties, DataStoreHelper}

/**
 * Abstract superclass for all top-level GeoMesa JCommander commands
 */
abstract class Command(parent: JCommander) {
  def execute()
  val params: Any
  def register = parent.addCommand(command, params)
  val command: String
}

/**
 * Abstract class for commands that have a pre-existing catlaog
 */
abstract class CommandWithCatalog(parent: JCommander) extends Command(parent) with AccumuloProperties {
  override val params: GeoMesaParams
  lazy val ds = new DataStoreHelper(params).getExistingStore
  lazy val catalog = params.catalog
}
