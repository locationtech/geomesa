/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.data

import com.beust.jcommander.{IValueValidator, Parameter, ParameterException, Parameters}
import org.locationtech.geomesa.kudu.KuduSystemProperties
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.kudu.tools.data.KuduCreateSchemaCommand.KuduCreateSchemaParams
import org.locationtech.geomesa.kudu.utils.ColumnConfiguration
import org.locationtech.geomesa.tools.data.CreateSchemaCommand
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.CreateSchemaParams
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

class KuduCreateSchemaCommand extends CreateSchemaCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduCreateSchemaParams()

  override protected def setBackendSpecificOptions(featureType: SimpleFeatureType): Unit =
    Option(params.compression).foreach(KuduSystemProperties.Compression.set)
}

object KuduCreateSchemaCommand {

  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class KuduCreateSchemaParams extends CreateSchemaParams with KuduParams {
    @Parameter(names = Array("--compression"),
      description = "Default compression for data files. One of 'lz4', 'snappy', 'zlib' or 'no_compression'", required = false, validateValueWith = classOf[CompressionTypeValidator])
    var compression: String = KuduSystemProperties.Compression.default
  }

  class CompressionTypeValidator extends IValueValidator[String] {
    override def validate(name: String, value: String): Unit = {
      try { ColumnConfiguration.compression(Option(value)) } catch {
        case NonFatal(_) =>
          throw new ParameterException("Invalid compression type.  Values types are " +
              "'lz4', 'snappy', 'zlib' or 'no_compression'")
      }
    }
  }
}
