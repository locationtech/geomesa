/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.lambda.tools.{LambdaDataStoreCommand, LambdaDataStoreParams}
import org.locationtech.geomesa.tools.data.{RemoveSchemaCommand, RemoveSchemaParams}

class LambdaRemoveSchemaCommand extends RemoveSchemaCommand[LambdaDataStore] with LambdaDataStoreCommand {
  override val params = new LambdaRemoveSchemaParams
}

@Parameters(commandDescription = "Remove a schema and all associated features")
class LambdaRemoveSchemaParams extends RemoveSchemaParams with LambdaDataStoreParams
