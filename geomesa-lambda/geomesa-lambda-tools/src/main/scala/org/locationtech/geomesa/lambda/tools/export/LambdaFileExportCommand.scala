/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.lambda.tools.{LambdaDataStoreCommand, LambdaDataStoreParams}
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}
import org.locationtech.geomesa.tools.export.{FileExportCommand, FileExportParams}

class LambdaFileExportCommand extends FileExportCommand[LambdaDataStore] with LambdaDataStoreCommand {
  override val params = new LambdaFileExportParams
}

@Parameters(commandDescription = "Export features from a GeoMesa data store")
class LambdaFileExportParams extends FileExportParams with LambdaDataStoreParams
    with RequiredTypeNameParam with OptionalIndexParam
