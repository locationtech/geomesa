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
import org.locationtech.geomesa.arrow.tools.{ArrowDataStoreCommand, UrlParam}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.{StatsBoundsCommand, StatsBoundsParams}

class ArrowStatsBoundsCommand extends StatsBoundsCommand[ArrowDataStore] with ArrowDataStoreCommand {
  override val params = new ArrowStatsBoundsParams

  override def execute(): Unit = {
    params.exact = true
    super.execute()
  }
}

@Parameters(commandDescription = "Calculate bounds on attributes in a GeoMesa feature type")
class ArrowStatsBoundsParams extends StatsBoundsParams with UrlParam with RequiredTypeNameParam
