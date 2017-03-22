/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}

trait BinExportCommand[DS <: GeoMesaDataStore[_, _, _]] extends DataStoreCommand[DS] with MethodProfiling {

  override val name = "export-bin"
  override def params: BinExportParams

  override def execute(): Unit = {
    implicit val timing = new Timing
    profile(withDataStore(export))
    Command.user.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
        s"in ${timing.time}ms")
  }

  protected def export(ds: DS): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val sft = ds.getSchema(params.featureName)
    val dtg = sft.getDtgField
    val attributes = ExportCommand.getAttributes(ds, DataFormats.Bin, params)
    val features = ExportCommand.getFeatureCollection(ds, DataFormats.Bin, attributes, params)
    val exporter = BinExporter(ExportCommand.createOutputStream(params.file, params.gzip), params, dtg)
    try {
      exporter.export(features)
      exporter.flush()
    } finally {
      IOUtils.closeQuietly(exporter)
    }
  }
}
