/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.kudu.tools.stats.KuduStatsTopKCommand.KuduStatsTopKParams
import org.locationtech.geomesa.tools.stats.{StatsTopKCommand, StatsTopKParams}

class KuduStatsTopKCommand extends StatsTopKCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduStatsTopKParams
}

object KuduStatsTopKCommand {
  @Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
  class KuduStatsTopKParams extends StatsTopKParams with KuduParams
}
