/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools

import org.locationtech.geomesa.tools._

object GeoToolsRunner extends Runner {

  override val name: String = "geomesa-gt"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new data.GeoToolsCreateSchemaCommand,
      new data.GeoToolsDeleteFeaturesCommand,
      new data.GeoToolsDescribeSchemaCommand,
      new data.GeoToolsGetSftConfigCommand,
      new data.GeoToolsGetTypeNamesCommand,
      new data.GeoToolsRemoveSchemaCommand,
      new data.GeoToolsUpdateSchemaCommand,
      new export.GeoToolsExportCommand,
      new export.GeoToolsPlaybackCommand,
      new ingest.GeoToolsIngestCommand
    )
  }
}
