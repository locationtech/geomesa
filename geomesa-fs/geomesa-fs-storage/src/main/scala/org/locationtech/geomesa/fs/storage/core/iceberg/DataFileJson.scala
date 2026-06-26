/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.fasterxml.jackson.core.{JsonFactory, JsonFactoryBuilder}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.iceberg.{ContentFileParser, DataFile, PartitionSpec}

import java.io.{InputStream, OutputStream};

object DataFileJson {

  private val factory =
    new JsonFactoryBuilder()
      .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
      .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false)
      .build()

  private val mapper = new ObjectMapper(factory)

  /**
   * Serialize a data file to json
   *
   * @param file data file
   * @param spec partition spec
   * @return
   */
  def serialize(file: DataFile, spec: PartitionSpec): String = ContentFileParser.toJson(file, spec)

  /**
   * Serialize a data file to json
   *
   * @param file data file
   * @param spec partition spec
   * @param out output to write to
   */
  def serialize(file: DataFile, spec: PartitionSpec, out: OutputStream): Unit =
    ContentFileParser.toJson(file, spec, factory.createGenerator(out))

  /**
   * Deserialize a data file from json
   *
   * @param spec partition spec
   * @param json json string
   * @return
   */
  def deserialize(spec: PartitionSpec, json: String): DataFile = deserialize(spec, mapper.readTree(json))

  /**
   * Deserialize a data file from json
   *
   * @param spec partition spec
   * @param json json input stream
   * @return
   */
  def deserialize(spec: PartitionSpec, json: InputStream): DataFile = deserialize(spec, mapper.readTree(json))

  private def deserialize(spec: PartitionSpec, node: JsonNode): DataFile =
    ContentFileParser.fromJson(node, spec).asInstanceOf[DataFile]
}
