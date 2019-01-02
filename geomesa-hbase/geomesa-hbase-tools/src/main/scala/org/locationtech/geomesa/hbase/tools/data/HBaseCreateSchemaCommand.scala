/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.data

import com.beust.jcommander.{IValueValidator, Parameter, ParameterException, Parameters}
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.hbase.tools.data.HBaseCreateSchemaCommand.HBaseCreateSchemaParams
import org.locationtech.geomesa.tools.data.CreateSchemaCommand
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.CreateSchemaParams
import org.opengis.feature.simple.SimpleFeatureType

class HBaseCreateSchemaCommand extends CreateSchemaCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseCreateSchemaParams()

  override protected def setBackendSpecificOptions(featureType: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
    Option(params.compression).foreach { c => featureType.setCompression(c) }
  }
}

object HBaseCreateSchemaCommand {
  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class HBaseCreateSchemaParams extends CreateSchemaParams with HBaseParams with ToggleRemoteFilterParam {
    @Parameter(names = Array("--compression"),
      description = "Enable compression for a feature.  One of \"snappy\", \"lzo\", \"gz\", \"bzip2\", \"lz4\", \"zstd\"", required = false, validateValueWith = classOf[CompressionTypeValidator])
    var compression: String = _
  }

  class CompressionTypeValidator extends IValueValidator[String] {
    val VALID_COMPRESSION_TYPES = Seq("snappy", "lzo", "gz", "bzip2", "lz4", "zstd")

    override def validate(name: String, value: String): Unit = {
      if (!VALID_COMPRESSION_TYPES.contains(value)) {
        throw new ParameterException(s"Invalid compression type.  Values types are ${VALID_COMPRESSION_TYPES.mkString(", ")}")
      }
    }
  }
}
