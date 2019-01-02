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
import org.locationtech.geomesa.kudu.tools.stats.KuduStatsBoundsCommand.KuduStatsBoundsParams
import org.locationtech.geomesa.tools.stats.{StatsBoundsCommand, StatsBoundsParams}

class KuduStatsBoundsCommand extends StatsBoundsCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduStatsBoundsParams
}

object KuduStatsBoundsCommand {
  @Parameters(commandDescription = "View or calculate bounds on attributes in a GeoMesa feature type")
  class KuduStatsBoundsParams extends StatsBoundsParams with KuduParams
}
