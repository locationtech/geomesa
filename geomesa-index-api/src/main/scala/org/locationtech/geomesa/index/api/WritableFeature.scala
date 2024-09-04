/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.kryo.serialization.IndexValueSerializer
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.security.SecurityUtils.FEATURE_VISIBILITY
import org.locationtech.geomesa.utils.index.VisibilityLevel

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
  * Wraps a simple feature for writing. Usually contains cached values that will be written to multiple indices,
  * to e.g. avoid re-serializing a simple feature multiple times
  */
trait WritableFeature {

  /**
    * Underlying simple feature
    *
    * @return
    */
  def feature: SimpleFeature

  /**
   * Key-value pairs representing this feature
   *
   * @return
   */
  def values: Seq[KeyValue]

  /**
   * Key-value pairs representing this feature, for reduced 'join' indices
   *
   * @return
   */
  def reducedValues: Seq[KeyValue]

  /**
    * Feature ID bytes
    */
  def id: Array[Byte]

  /**
    * Hash of the simple feature ID - can be used for sharding.
    *
    * Note: we could use the idBytes here, but for back compatibility of deletes we don't want to change it
    */
  lazy val idHash: Int = Math.abs(MurmurHash3.stringHash(feature.getID))

  /**
    * Convenience method for getting an attribute from the underlying feature
    *
    * @param i index of the attribute
    * @tparam T type of the attribute
    * @return
    */
  def getAttribute[T](i: Int): T = feature.getAttribute(i).asInstanceOf[T]
}

object WritableFeature {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val EmptyBytes = Array.empty[Byte]

  /**
    * Creates a function for wrapping a feature into a WritableFeature
    *
    * @param sft simple feature type
    * @param groups column groups
    * @return
    */
  def wrapper(sft: SimpleFeatureType, groups: ColumnGroups): FeatureWrapper[WritableFeature] = {
    val idSerializer: String => Array[Byte] = GeoMesaFeatureIndex.idToBytes(sft)
    val serializers: Seq[(Array[Byte], SimpleFeatureSerializer)] = groups.serializers(sft)
    val indexSerializer: SimpleFeatureSerializer = IndexValueSerializer(sft)

    sft.getVisibilityLevel match {
      case VisibilityLevel.Feature =>
        new FeatureLevelFeatureWrapper(serializers, indexSerializer, idSerializer)

      case VisibilityLevel.Attribute =>
        val Seq((colFamily, serializer)) = serializers // there should only be 1
        new AttributeLevelFeatureWrapper(colFamily, serializer, indexSerializer, idSerializer)
    }
  }

  /**
    * Creates a writable feature from a feature
    */
  trait FeatureWrapper[+T <: WritableFeature] {

    /**
      * Create a writable feature
      *
      * @param feature feature
      * @param delete true if the feature is an already written feature that we are deleting
      * @return
      */
    def wrap(feature: SimpleFeature, delete: Boolean = false): T
  }

  /**
   * Wrapper that supports feature-level visibility
   *
   * @param serializers feature serializers, per column group
   * @param indexValueSerializer index value serializer
   * @param idSerializer feature id serializer
   */
  private class FeatureLevelFeatureWrapper(
      serializers: Seq[(Array[Byte], SimpleFeatureSerializer)],
      indexValueSerializer: SimpleFeatureSerializer,
      idSerializer: String => Array[Byte]
    ) extends FeatureWrapper[WritableFeature] {
    override def wrap(feature: SimpleFeature, delete: Boolean): WritableFeature =
      new FeatureLevelWritableFeature(feature, serializers, indexValueSerializer, idSerializer)
  }

  /**
   * Wrapper that supports attribute-level visibilities
   *
   * @param colFamily attribute vis col family
   * @param serializer serializer
   * @param indexValueSerializer index value serializer
   * @param idSerializer feature id serializer
   */
  private class AttributeLevelFeatureWrapper(
      colFamily: Array[Byte],
      serializer: SimpleFeatureSerializer,
      indexValueSerializer: SimpleFeatureSerializer,
      idSerializer: String => Array[Byte]
    ) extends FeatureWrapper[WritableFeature] {
    override def wrap(feature: SimpleFeature, delete: Boolean): WritableFeature =
      new AttributeLevelWritableFeature(feature, colFamily, serializer, indexValueSerializer, idSerializer)
  }

  /**
    * Writable feature for feature-level visibilities
    *
    * @param feature simple feature
    * @param serializers serializers, per column group
    * @param idSerializer feature id serializer
    */
  private class FeatureLevelWritableFeature(
      val feature: SimpleFeature,
      serializers: Seq[(Array[Byte], SimpleFeatureSerializer)],
      indexValueSerializer: SimpleFeatureSerializer,
      idSerializer: String => Array[Byte]
    ) extends WritableFeature {

    private lazy val visibility = {
      val vis = feature.getUserData.get(FEATURE_VISIBILITY).asInstanceOf[String]
      if (vis == null) { EmptyBytes } else { vis.getBytes(StandardCharsets.UTF_8) }
    }

    private lazy val indexValue = indexValueSerializer.serialize(feature)

    override lazy val id: Array[Byte] = idSerializer.apply(feature.getID)

    override lazy val values: Seq[KeyValue] = serializers.map { case (colFamily, serializer) =>
      KeyValue(colFamily, EmptyBytes, visibility, serializer.serialize(feature))
    }

    override lazy val reducedValues: Seq[KeyValue] = values.map(_.copy(toValue = indexValue))
  }

  /**
    * Writable feature for attribute-level visibilities
    *
    * @param feature simple feature
    * @param colFamily attribute vis col family
    * @param serializer serializer
    * @param idSerializer feature id serializer
    */
  private class AttributeLevelWritableFeature(
      val feature: SimpleFeature,
      colFamily: Array[Byte],
      serializer: SimpleFeatureSerializer,
      indexValueSerializer: SimpleFeatureSerializer,
      idSerializer: String => Array[Byte]
    ) extends WritableFeature {

    private lazy val indexGroups: Seq[(Array[Byte], Array[Byte], SimpleFeature)] = {
      val attributeCount = feature.getFeatureType.getAttributeCount
      val userData = feature.getUserData.get(FEATURE_VISIBILITY).asInstanceOf[String]
      val grouped = scala.collection.mutable.Map.empty[String, ArrayBuffer[Byte]]
      if (userData == null || userData.isEmpty) {
        grouped += "" -> ArrayBuffer.tabulate[Byte](attributeCount)(_.toByte)
      } else {
        val visibilities = userData.split(",")
        require(visibilities.length == feature.getFeatureType.getAttributeCount,
          s"Per-attribute visibilities do not match feature type (${feature.getFeatureType.getAttributeCount} values expected): $userData")
        var i = 0
        while (i < visibilities.length) {
          grouped.getOrElseUpdate(visibilities(i), ArrayBuffer.empty[Byte]) += i.toByte
          i += 1
        }
      }
      grouped.toSeq.map { case (vis, builder) =>
        val indices = builder.toArray
        val sf = new ScalaSimpleFeature(feature.getFeatureType, "")
        indices.foreach(i => sf.setAttributeNoConvert(i, feature.getAttribute(i)))
        (vis.getBytes(StandardCharsets.UTF_8), indices, sf)
      }
    }

    override lazy val id: Array[Byte] = idSerializer.apply(feature.getID)

    override lazy val values: Seq[KeyValue] =
      indexGroups.map { case (vis, indices, sf) => KeyValue(colFamily, indices, vis, serializer.serialize(sf)) }

    override lazy val reducedValues: Seq[KeyValue] =
      indexGroups.map { case (vis, indices, sf) => KeyValue(colFamily, indices, vis, indexValueSerializer.serialize(sf)) }
  }
}
