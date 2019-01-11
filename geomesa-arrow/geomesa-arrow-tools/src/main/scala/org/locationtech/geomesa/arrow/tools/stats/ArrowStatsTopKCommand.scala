/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.arrow.data.ArrowDataStore
import org.locationtech.geomesa.arrow.tools.ArrowDataStoreCommand
import org.locationtech.geomesa.arrow.tools.UrlParam
import org.locationtech.geomesa.tools.stats.{StatsTopKCommand, StatsTopKParams}

class ArrowStatsTopKCommand extends StatsTopKCommand[ArrowDataStore] with ArrowDataStoreCommand {

  override val params = new ArrowStatsTopKParams

  override def execute(): Unit = {
    params.exact = true
    super.execute()
  }
}

@Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
class ArrowStatsTopKParams extends StatsTopKParams with UrlParam
