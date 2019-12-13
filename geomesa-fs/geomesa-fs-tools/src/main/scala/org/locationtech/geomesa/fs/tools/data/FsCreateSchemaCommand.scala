/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/***********************************************************************
  * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  ***********************************************************************/

package org.locationtech.geomesa.fs.tools.data

import com.beust.jcommander.{ParameterException, Parameters}
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.StorageKeys
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, OptionalEncodingParam, OptionalSchemeParams}
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand.FsCreateSchemaParams
import org.locationtech.geomesa.fs.tools.utils.PartitionSchemeArgResolver
import org.locationtech.geomesa.tools.data.CreateSchemaCommand
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.CreateSchemaParams
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ListBuffer

class FsCreateSchemaCommand extends CreateSchemaCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params: FsCreateSchemaParams = new FsCreateSchemaParams

  override protected def setBackendSpecificOptions(sft: SimpleFeatureType): Unit =
    FsCreateSchemaCommand.setOptions(sft, params)
}

object FsCreateSchemaCommand {

  import scala.collection.JavaConverters._

  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class FsCreateSchemaParams
      extends CreateSchemaParams with FsParams with OptionalEncodingParam with OptionalSchemeParams

  def setOptions(sft: SimpleFeatureType, params: OptionalEncodingParam with OptionalSchemeParams): Unit = {
    import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

    val errors = ListBuffer.empty[String]

    if (params.scheme == null) {
      if (sft.getUserData.get(StorageKeys.SchemeKey) == null) {
        errors += "--partition-scheme"
      }
    } else {
      PartitionSchemeArgResolver.resolve(sft, params.scheme) match {
        case Left(e) => throw new ParameterException(e)
        case Right(s) => sft.setScheme(s.name, s.options)
      }
    }
    sft.setLeafStorage(params.leafStorage)

    if (params.encoding == null) {
      if (sft.getUserData.get(StorageKeys.EncodingKey) == null) {
        errors += "--encoding, -e"
      }
    } else {
      sft.setEncoding(params.encoding)
    }

    if (errors.nonEmpty) {
      throw new ParameterException(s"The following options are required: ${errors.mkString(" ")}")
    }

    // Can use this to set things like compression and summary levels for parquet in the sft user data
    // to be picked up by the ingest job
    params.storageOpts.asScala.foreach { case (k, v) => sft.getUserData.put(k,v) }
  }
}
