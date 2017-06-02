/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.tools.CatalogParam
import org.locationtech.geomesa.tools.data.{CreateSchemaCommand, CreateSchemaParams}

class HBaseCreateSchemaCommand extends CreateSchemaCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseCreateSchemaParams()
}

@Parameters(commandDescription = "Create a GeoMesa feature type")
class HBaseCreateSchemaParams extends CreateSchemaParams with CatalogParam
