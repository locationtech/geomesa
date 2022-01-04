/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools

import org.locationtech.geomesa.tools.{Command, Runner}

object KuduRunner extends Runner {

  override val name: String = "geomesa-kudu"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new data.KuduCreateSchemaCommand,
      new data.KuduDeleteCatalogCommand,
      new data.KuduRemoveSchemaCommand,
      new data.KuduUpdateSchemaCommand,
      new export.KuduExportCommand,
      new ingest.KuduDeleteFeaturesCommand,
      new ingest.KuduIngestCommand,
      new status.KuduDescribeSchemaCommand,
      new status.KuduExplainCommand,
      new status.KuduGetTypeNamesCommand,
      new status.KuduGetSftConfigCommand,
      new stats.KuduStatsBoundsCommand,
      new stats.KuduStatsCountCommand,
      new stats.KuduStatsTopKCommand,
      new stats.KuduStatsHistogramCommand
    )
  }
}
