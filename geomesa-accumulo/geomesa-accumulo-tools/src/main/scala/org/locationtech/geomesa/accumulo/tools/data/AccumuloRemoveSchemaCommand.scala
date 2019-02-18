/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.data.{RemoveSchemaCommand, RemoveSchemaParams}

class AccumuloRemoveSchemaCommand extends RemoveSchemaCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloRemoveSchemaParams
}

@Parameters(commandDescription = "Remove a schema and all associated features")
class AccumuloRemoveSchemaParams extends RemoveSchemaParams with AccumuloDataStoreParams
