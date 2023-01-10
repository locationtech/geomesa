/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro
package serialization

import org.apache.avro.Schema
import org.apache.avro.io.{DatumReader, Decoder}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.serialization.SimpleFeatureDatumReader.VersionedFields

/**
 * Avro reader for simple features. Due to avro lifecycles, the following methods must be
 * called in order before use:
 *
 * 1) setSchema (if available)
 * 2) setFeatureType
 */
class SimpleFeatureDatumReader extends DatumReader[SimpleFeature] {

  import AvroField._

  import scala.collection.JavaConverters._

  private val versionedFields = Array.ofDim[VersionedFields](SerializationVersions.MaxVersion)

  private var schema: Schema = _
  private var sft: SimpleFeatureType = _
  private var fid: Option[AvroField[String]] = None
  private var userData: Option[AvroField[java.util.Map[AnyRef, AnyRef]]] = None
  private var fields: Seq[AvroField[AnyRef]] = Seq.empty

  override def setSchema(schema: Schema): Unit = {
    require(this.schema == null || this.schema == schema, "setSchema can only be called once")
    require(
      schema.getFields.size > 0 && schema.getFields.get(0).name() == VersionField.name,
      s"Unexpected schema: $schema")
    this.schema = schema
  }

  /**
   * Set the feature type. This must be called before the reader can be used
   *
   * @param sft simple feature type
   */
  def setFeatureType(sft: SimpleFeatureType): Unit = {
    // ideally, we could re-construct the feature type from the schema... going forward, that should be possible,
    // but to support older data files we still need this
    require(schema != null, "setSchema must be called before setFeatureType")
    this.sft = sft
    this.fid = if (schema.getField(FidField.name) != null) { Some(FidField) } else { None }
    this.userData = if (schema.getField(UserDataField.name) != null) { Some(UserDataField) } else { None }
    this.fields = sft.getAttributeDescriptors.asScala.map(AvroField.apply).toSeq
  }

  override def read(reuse: SimpleFeature, in: Decoder): SimpleFeature = {
    val version = VersionField.read(in)
    var fields = versionedFields(version - 1)
    if (fields == null) {
      val fid = this.fid.map(_.withVersion(version)).orNull
      val userData = this.userData.map(_.withVersion(version)).orNull
      fields = VersionedFields(fid, userData, this.fields.map(_.withVersion(version)).toArray)
      versionedFields(version - 1) = fields
    }
    val id = if (fields.fid == null) { "" } else { fields.fid.read(in) }
    val sf = reuse match {
      case f: ScalaSimpleFeature if sft.eq(f.getFeatureType) => f.setId(id); f.getUserData.clear(); f
      case _ => new ScalaSimpleFeature(sft, id)
    }
    var i = 0
    while (i < sft.getAttributeCount) {
      sf.setAttributeNoConvert(i, fields.fields(i).read(in))
      i += 1
    }
    if (fields.userData != null) {
      sf.getUserData.putAll(fields.userData.read(in))
    }
    sf
  }
}

object SimpleFeatureDatumReader {

  /**
   * Create and initialize a reader, when the schema is known up front
   *
   * @param schema schema
   * @param sft feature type
   * @return
   */
  def apply(schema: Schema, sft: SimpleFeatureType): SimpleFeatureDatumReader = {
    val reader = new SimpleFeatureDatumReader()
    reader.setSchema(schema)
    reader.setFeatureType(sft)
    reader
  }

  private case class VersionedFields(
      fid: AvroField[String],
      userData: AvroField[java.util.Map[AnyRef, AnyRef]],
      fields: Array[AvroField[AnyRef]]
    )
}
