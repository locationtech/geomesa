/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools

import org.locationtech.geomesa.tools.{Command, Runner}

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
      new data.PostgisUpgradeSchemaCommand(),
      new export.GeoToolsExportCommand,
      new export.GeoToolsPlaybackCommand,
      new ingest.GeoToolsIngestCommand
    )
  }
}
