/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import org.apache.avro.Schema
import org.apache.avro.io.{DatumWriter, Encoder}
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Datum writer for simple features
 *
 * @param sft simple feature type to write
 * @param opts serialization options
 */
class SimpleFeatureDatumWriter(sft: SimpleFeatureType, opts: Set[SerializationOption] = Set.empty)
    extends DatumWriter[SimpleFeature] {

  private val serde = AvroSerialization(sft, opts)

  private val fid = serde.fid.orNull
  private val fields = serde.fields.toArray
  private val userData = serde.userData.orNull

  private var schema = serde.schema

  def getSchema: Schema = schema

  override def setSchema(schema: Schema): Unit = this.schema = schema

  override def write(datum: SimpleFeature, out: Encoder): Unit = {
    AvroField.VersionField.write(out, serde.version)
    if (fid != null) {
      fid.write(out, datum.getID)
    }
    var i = 0
    while (i < sft.getAttributeCount) {
      fields(i).write(out, datum.getAttribute(i))
      i += 1
    }
    if (userData != null) {
      userData.write(out, datum.getUserData)
    }
  }
}
