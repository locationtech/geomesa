/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.fasterxml.jackson.core.{JsonFactory, JsonFactoryBuilder}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.iceberg.{ContentFileParser, DataFile, PartitionSpec}

import java.io.{InputStream, OutputStream};

object DataFileJson {

  private val factory =
    new JsonFactoryBuilder()
      .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
      .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false)
      .build()

  private val mapper = new ObjectMapper(factory)

  def serialize(out: OutputStream, file: DataFile, spec: PartitionSpec): Unit =
    ContentFileParser.toJson(file, spec, factory.createGenerator(out))

  def deserialize(in: InputStream, spec: PartitionSpec): DataFile = {
    val jsonNode = mapper.readTree(in)
    ContentFileParser.fromJson(jsonNode, spec).asInstanceOf[DataFile]
  }
}
