/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.common.commands

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils
import org.locationtech.geomesa.tools.common.commands.SFT2AvroSchemaCommand.SFT2AvroSchemaCommandParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader

class SFT2AvroSchemaCommand(parent: JCommander) extends Command(parent) {

  override val command = "sft2avro"
  val params = new SFT2AvroSchemaCommandParams

  override def execute() = {
    val sft = SimpleFeatureTypeLoader.sftForName(params.sft)
    if(sft.isDefined) {
      val schema = AvroSimpleFeatureUtils.generateSchema(sft.get, withUserData = true, mangleNames = false)
      println(schema.toString(true))
    } else {
      println(s"Could not find feature type ${params.sft}")
    }
  }

}

object SFT2AvroSchemaCommand {
  @Parameters(commandDescription = "Convert SimpleFeatureTypes to Avro schemas")
  class SFT2AvroSchemaCommandParams {
    @Parameter(names = Array("-s", "--sft"), description = "SimpleFeatureTypeName", required = true)
    var sft: String = null
  }
}
