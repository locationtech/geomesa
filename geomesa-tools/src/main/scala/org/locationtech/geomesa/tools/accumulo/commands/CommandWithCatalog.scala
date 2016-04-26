/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.accumulo.{AccumuloProperties, DataStoreHelper, GeoMesaConnectionParams}
import org.locationtech.geomesa.tools.common.commands.Command

/**
 * Abstract class for commands that have a pre-existing catalog
 */
abstract class CommandWithCatalog(parent: JCommander) extends Command(parent) with AccumuloProperties {
  override val params: GeoMesaConnectionParams
  lazy val ds = new DataStoreHelper(params).getDataStore
  lazy val catalog = params.catalog
}
