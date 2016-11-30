/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.utils.geotools.SftArgResolver

class GenerateAvroSchemaCommand extends Command {

  override val name = "gen-avro-schema"
  val params = new GenerateAvroSchemaParams

  override def execute(): Unit = {
    val sft = SftArgResolver.getSft(params.spec, params.featureName)
    sft match {
      case Right(sft) =>
        val schema = AvroSimpleFeatureUtils.generateSchema(sft, withUserData = true)
        Command.output.info(schema.toString(true))
      case Left(error) =>
        Command.user.warn(error)
    }
  }

}

@Parameters(commandDescription = "Generate an Avro schema from a SimpleFeatureType")
class GenerateAvroSchemaParams extends RequiredFeatureSpecParam with OptionalTypeNameParam
