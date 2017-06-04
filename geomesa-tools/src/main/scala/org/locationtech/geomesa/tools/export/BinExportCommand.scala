/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.export.BinExportCommand.BinExportParams
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.stats.MethodProfiling

@deprecated("ExportCommand")
trait BinExportCommand[DS <: GeoMesaDataStore[_, _, _]] extends DataStoreCommand[DS] with MethodProfiling {

  override val name = "export-bin"
  override def params: BinExportParams

  def delegate: ExportCommand[DS]

  override def execute(): Unit = {
    Command.user.warn(s"This operation has been deprecated. Use the 'export' command instead.")
    val d = delegate
    d.params.attributes   = params.attributes
    d.params.file         = params.file
    d.params.gzip         = params.gzip
    d.params.maxFeatures  = params.maxFeatures
    d.params.noHeader     = params.noHeader
    d.params.outputFormat = params.outputFormat
    d.params.catalog      = params.catalog
    d.params.cqlFilter    = params.cqlFilter
    d.params.featureName  = params.featureName
    d.params.index        = params.index

    d.params.hints = new java.util.HashMap[String, String]()
    Option(params.hints).foreach(d.params.hints.putAll)
    Option(params.idAttribute).foreach(d.params.hints.put("BIN_TRACK", _))
    Option(params.geomAttribute).foreach(d.params.hints.put("BIN_GEOM", _))
    Option(params.dateAttribute).foreach(d.params.hints.put("BIN_DTG", _))
    Option(params.labelAttribute).foreach(d.params.hints.put("BIN_LABEL", _))

    d.execute()
  }
}

object BinExportCommand {

  trait BinExportParams extends ExportParams {
    @Parameter(names = Array("--id-attribute"), description = "Name of the id attribute to export")
    var idAttribute: String = _

    @Parameter(names = Array("--geom-attribute"), description = "Name of the geometry attribute to export")
    var geomAttribute: String = _

    @Parameter(names = Array("--label-attribute"), description = "Name of the attribute to use as a bin file label")
    var labelAttribute: String = _

    @Parameter(names = Array("--dt-attribute"), description = "Name of the date attribute to export")
    var dateAttribute: String = _
  }
}