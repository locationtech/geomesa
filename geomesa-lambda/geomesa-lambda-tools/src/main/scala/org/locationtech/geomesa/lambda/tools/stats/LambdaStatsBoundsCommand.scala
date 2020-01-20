/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.lambda.tools.stats.LambdaStatsBoundsCommand.LambdaStatsBoundsParams
import org.locationtech.geomesa.lambda.tools.{LambdaDataStoreCommand, LambdaDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.StatsBoundsCommand
import org.locationtech.geomesa.tools.stats.StatsBoundsCommand.StatsBoundsParams

class LambdaStatsBoundsCommand extends StatsBoundsCommand[LambdaDataStore] with LambdaDataStoreCommand {
  override val params = new LambdaStatsBoundsParams
}

object LambdaStatsBoundsCommand {
  @Parameters(commandDescription = "View or calculate bounds on attributes in a GeoMesa feature type")
  class LambdaStatsBoundsParams extends StatsBoundsParams with LambdaDataStoreParams with RequiredTypeNameParam
}
