/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro
package serialization

import org.apache.avro.{Schema, SchemaBuilder}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.serialization.AvroField.VersionField

/**
 * Config used for serializing simple features
 *
 * @param version serialization version, see org.locationtech.geomesa.features.avro.SerializationVersions
 * @param schema the avro schema
 * @param fid the feature id field, if present
 * @param fields fields for each attribute in the feature
 * @param userData the user data field, if present
 */
case class AvroSerialization(
    version: Int,
    schema: Schema,
    fid: Option[AvroField[String]],
    fields: Seq[AvroField[AnyRef]],
    userData: Option[AvroField[java.util.Map[AnyRef, AnyRef]]]
  )

object AvroSerialization {

  import org.locationtech.geomesa.features.avro.serialization.AvroField.{FidField, UserDataField}
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  // array of name encoders, indexed by the encoder version (minus 1)
  private val nameEncoders = Array.tabulate(SerializationVersions.MaxVersion)(i => new FieldNameEncoder(i + 1))

  /**
   * Create a serialization config
   *
   * @param sft simple feature type
   * @param opts options
   * @return
   */
  def apply(sft: SimpleFeatureType, opts: Set[SerializationOption]): AvroSerialization = {
    val version: Int =
      if (opts.useNativeCollections && sft.getAttributeDescriptors.asScala.exists(d => d.isList || d.isMap)) {
        SerializationVersions.NativeCollectionVersion
      } else {
        SerializationVersions.DefaultVersion
      }

    val fields = sft.getAttributeDescriptors.asScala.map(AvroField.apply(_).withVersion(version)).toSeq

    val fid = if (opts.withoutId) { None } else { Some(FidField.withVersion(version)) }
    val userData = if (opts.withUserData) { Some(UserDataField.withVersion(version)) } else { None }

    val schema = AvroSerialization.schema(sft, version, fields, fid.isDefined, userData.isDefined)

    AvroSerialization(version, schema, fid, fields, userData)
  }

  /**
   * Calculate the schema corresponding to a given feature type
   *
   * @param sft feature type
   * @param version serialization version
   * @param fields field definitions
   * @param includeFid include feature id
   * @param includeUserData include user data
   * @return
   */
  def schema(
      sft: SimpleFeatureType,
      version: Int,
      fields: Seq[AvroField[AnyRef]],
      includeFid: Boolean,
      includeUserData: Boolean): Schema = {

    import AvroField.{FidField, UserDataField, VersionField}

    require(version > 0 && version <= SerializationVersions.MaxVersion,
      s"Unknown version $version - valid versions are 1 to ${SerializationVersions.MaxVersion}")
    require(fields.length == sft.getAttributeCount, "Mismatch between fields and feature type")

    val nameEncoder = nameEncoders(version - 1)
    val builder =
      SchemaBuilder.record(nameEncoder.encode(sft.getTypeName))
          .namespace(Option(sft.getName.getNamespaceURI).getOrElse(AvroNamespace))
          .fields

    builder.name(VersionField.name).`type`(VersionField.schema).withDefault(version)
    if (includeFid) {
      builder.name(FidField.name).`type`(FidField.schema).noDefault
    }

    val types = fields.map(_.schema).iterator
    sft.getAttributeDescriptors.asScala.foreach { d =>
      builder.name(nameEncoder.encode(d.getLocalName)).`type`(types.next).noDefault()
    }

    if (includeUserData) {
      builder.name(UserDataField.name).`type`(UserDataField.schema).noDefault()
    }

    builder.endRecord()
  }

  /**
   * For a given schema, checks if collections are encoded natively or as binary
   *
   * @param schema schema
   * @return
   */
  def usesNativeCollections(schema: Schema): Boolean = {
    Option(schema.getField(VersionField.name))
        .exists(_.defaultVal() == SerializationVersions.NativeCollectionVersion)
  }
}
