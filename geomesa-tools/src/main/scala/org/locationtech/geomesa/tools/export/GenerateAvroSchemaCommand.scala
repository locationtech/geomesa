/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import com.beust.jcommander.{Parameter, Parameters}
import org.locationtech.geomesa.features.SerializationOption
import org.locationtech.geomesa.features.avro.serialization.AvroSerialization
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.GenerateAvroSchemaCommand.GenerateAvroSchemaParams
import org.locationtech.geomesa.tools.utils.CLArgResolver

class GenerateAvroSchemaCommand extends Command {

  override val name = "gen-avro-schema"
  val params = new GenerateAvroSchemaParams

  override def execute(): Unit = {
    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    val builder = SerializationOption.builder.withUserData
    if (params.native) {
      builder.withNativeCollections
    }
    if (params.kafka) {
      builder.withoutId
    }
    val schema = AvroSerialization(sft, builder.build()).schema
    Command.output.info(schema.toString(true))
  }
}

object GenerateAvroSchemaCommand {

  @Parameters(commandDescription = "Generate an Avro schema from a SimpleFeatureType")
  class GenerateAvroSchemaParams extends RequiredFeatureSpecParam with OptionalTypeNameParam {
    @Parameter(names = Array("--use-native-collections"),
      description = "Encode list and map type attributes as native Avro records")
    var native: Boolean = false

    @Parameter(names = Array("--kafka-compatible"),
      description = "Exclude the feature ID, which aligns with the schema used for GeoMesa Kafka topics")
    var kafka: Boolean = false
  }
}
