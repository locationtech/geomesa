/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.arrow.data.ArrowDataStore
import org.locationtech.geomesa.arrow.tools.ArrowDataStoreCommand
import org.locationtech.geomesa.arrow.tools.ArrowDataStoreCommand.UrlParam
import org.locationtech.geomesa.arrow.tools.stats.ArrowStatsCountCommand.ArrowStatsCountParams
import org.locationtech.geomesa.tools.ProvidedTypeNameParam
import org.locationtech.geomesa.tools.stats.StatsCountCommand
import org.locationtech.geomesa.tools.stats.StatsCountCommand.StatsCountParams

class ArrowStatsCountCommand extends StatsCountCommand[ArrowDataStore] with ArrowDataStoreCommand {

  override val params = new ArrowStatsCountParams

  override def execute(): Unit = {
    params.exact = true
    super.execute()
  }
}

object ArrowStatsCountCommand {
  @Parameters(commandDescription = "Calculate feature counts in a GeoMesa feature type")
  class ArrowStatsCountParams extends StatsCountParams with UrlParam with ProvidedTypeNameParam
}
