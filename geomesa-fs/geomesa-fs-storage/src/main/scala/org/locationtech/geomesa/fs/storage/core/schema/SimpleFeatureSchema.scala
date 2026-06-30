/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.schema

object SimpleFeatureSchema {

  val InternalFieldDelimiter = "__"

  val FeatureIdField    = s"${InternalFieldDelimiter}fid$InternalFieldDelimiter"
  val VisibilitiesField = s"${InternalFieldDelimiter}vis$InternalFieldDelimiter"

  val SftNameKey            = "geomesa.fs.sft.name"
  val SftSpecKey            = "geomesa.fs.sft.spec"
  val GeometryEncodingKey   = "geomesa.parquet.geometries"
  val PartitionKey          = "geomesa.fs.partition"
}
