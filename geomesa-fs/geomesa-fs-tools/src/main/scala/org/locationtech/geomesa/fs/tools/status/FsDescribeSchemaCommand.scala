/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.Parameters
import com.typesafe.scalalogging.Logger
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.status.FsDescribeSchemaCommand.FsDescribeSchemaParams
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.status.DescribeSchemaCommand

class FsDescribeSchemaCommand extends DescribeSchemaCommand[FileSystemDataStore] with FsDataStoreCommand {
  override val params = new FsDescribeSchemaParams

  override protected def describe(ds: FileSystemDataStore, sft: SimpleFeatureType, logger: Logger): Unit = {
    super.describe(ds, sft, logger)
    val metadata = ds.storage(sft.getTypeName).metadata
    logger.info(s"\nPartition scheme | ${metadata.scheme.pattern}")
    logger.info(s"File encoding    | ${metadata.encoding}")
    logger.info(s"Leaf storage     | ${metadata.leafStorage}")
  }
}

object FsDescribeSchemaCommand {
  @Parameters(commandDescription = "Describe the attributes of a given GeoMesa feature type")
  class FsDescribeSchemaParams extends FsParams with RequiredTypeNameParam
}
