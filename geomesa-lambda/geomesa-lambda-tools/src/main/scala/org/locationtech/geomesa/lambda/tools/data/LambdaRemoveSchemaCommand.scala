/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
