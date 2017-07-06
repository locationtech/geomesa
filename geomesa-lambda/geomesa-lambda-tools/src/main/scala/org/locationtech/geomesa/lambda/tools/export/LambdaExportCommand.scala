/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.lambda.tools.{LambdaDataStoreCommand, LambdaDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.export.{ExportCommand, ExportParams}

class LambdaExportCommand extends ExportCommand[LambdaDataStore] with LambdaDataStoreCommand {
  override val params = new LambdaExportParams
}

@Parameters(commandDescription = "Export features from a GeoMesa data store")
class LambdaExportParams extends ExportParams with LambdaDataStoreParams with RequiredTypeNameParam
