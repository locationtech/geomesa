/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.lambda.tools.stats.LambdaStatsTopKCommand.LambdaStatsTopKParams
import org.locationtech.geomesa.lambda.tools.{LambdaDataStoreCommand, LambdaDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.StatsTopKCommand
import org.locationtech.geomesa.tools.stats.StatsTopKCommand.StatsTopKParams

class LambdaStatsTopKCommand extends StatsTopKCommand[LambdaDataStore] with LambdaDataStoreCommand {
  override val params = new LambdaStatsTopKParams
}

object LambdaStatsTopKCommand {
  @Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
  class LambdaStatsTopKParams extends StatsTopKParams with LambdaDataStoreParams with RequiredTypeNameParam
}
