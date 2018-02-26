/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools.export

import com.beust.jcommander.Parameters
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.arrow.data.ArrowDataStore
import org.locationtech.geomesa.arrow.tools.{ArrowDataStoreCommand, UrlParam}
import org.locationtech.geomesa.tools.export.{FileExportCommand, FileExportParams}
import org.opengis.feature.simple.SimpleFeatureType

class ArrowFileExportCommand extends FileExportCommand[ArrowDataStore] with ArrowDataStoreCommand {

  override val params = new ArrowFileExportParams

  override protected def getSchema(ds: ArrowDataStore): SimpleFeatureType = ds.getSchema
  override protected def getFeatures(ds: ArrowDataStore, query: Query): SimpleFeatureCollection =
    ds.getFeatureSource().getFeatures(query)
}

@Parameters(commandDescription = "Export features from a GeoMesa data store")
class ArrowFileExportParams extends FileExportParams with UrlParam
