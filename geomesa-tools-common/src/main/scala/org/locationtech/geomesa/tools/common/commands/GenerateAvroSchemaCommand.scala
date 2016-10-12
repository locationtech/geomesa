/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.common.commands

import com.beust.jcommander.{JCommander, Parameters}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils
import org.locationtech.geomesa.tools.common.{OptionalFeatureTypeNameParam, FeatureTypeSpecParam}
import org.locationtech.geomesa.tools.common.commands.GenerateAvroSchemaCommand.GenerateAvroSchemaParams
import org.locationtech.geomesa.utils.geotools.SftArgResolver

class GenerateAvroSchemaCommand(parent: JCommander) extends Command(parent) {

  override val command = "gen-avroschema"
  val params = new GenerateAvroSchemaParams

  override def execute() = {
    val sft = SftArgResolver.getSft(params.spec, params.featureName)
    if (sft.isDefined) {
      val schema = AvroSimpleFeatureUtils.generateSchema(sft.get, withUserData = true)
      println(schema.toString(true))
    } else {
      println(s"Could not find feature type ${params.spec}")
    }
  }

}

object GenerateAvroSchemaCommand {
  @Parameters(commandDescription = "Generate an Avro schema from a SimpleFeatureType")
  class GenerateAvroSchemaParams extends FeatureTypeSpecParam with OptionalFeatureTypeNameParam
}
