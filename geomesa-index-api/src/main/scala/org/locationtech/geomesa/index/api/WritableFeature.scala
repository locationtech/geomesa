/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.security.SecurityUtils.FEATURE_VISIBILITY
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
  def wrapper(sft: SimpleFeatureType, groups: ColumnGroups): FeatureWrapper = {
    val idSerializer: String => Array[Byte] = GeoMesaFeatureIndex.idToBytes(sft)
    val serializers: Seq[(Array[Byte], SimpleFeatureSerializer)] = groups.serializers(sft)

    sft.getVisibilityLevel match {
      case VisibilityLevel.Feature =>
        new FeatureLevelFeatureWrapper(serializers, idSerializer)

      case VisibilityLevel.Attribute =>
        val Seq((colFamily, serializer)) = serializers // there should only be 1
        new AttributeLevelFeatureWrapper(colFamily, serializer, idSerializer)
    }
  }

  /**
    * Creates a writable feature from a feature
    */
  trait FeatureWrapper {

    /**
      * Create a writable feature
      *
      * @param feature feature
      * @return
      */
    def wrap(feature: SimpleFeature): WritableFeature
  }

  /**
    * Wrapper that supports feature-level visibility
    *
    * @param serializers feature serializers, per column group
    * @param idSerializer feature id serializer
    */
  class FeatureLevelFeatureWrapper(serializers: Seq[(Array[Byte], SimpleFeatureSerializer)],
                                   idSerializer: String => Array[Byte]) extends FeatureWrapper {
    override def wrap(feature: SimpleFeature): WritableFeature =
      new FeatureLevelWritableFeature(feature, serializers, idSerializer)
  }

  /**
    * Wrapper that supports attribute-level visibilities
    *
    * @param colFamily attribute vis col family
    * @param serializer serializer
    * @param idSerializer feature id serializer
    */
  class AttributeLevelFeatureWrapper(colFamily: Array[Byte],
                                     serializer: SimpleFeatureSerializer,
                                     idSerializer: String => Array[Byte]) extends FeatureWrapper {
    override def wrap(feature: SimpleFeature): WritableFeature =
      new AttributeLevelWritableFeature(feature, colFamily, serializer, idSerializer)
  }

  /**
    * Writable feature for feature-level visibilities
    *
    * @param feature simple feature
    * @param serializers serializers, per column group
    * @param idSerializer feature id serializer
    */
  class FeatureLevelWritableFeature(val feature: SimpleFeature,
                                    serializers: Seq[(Array[Byte], SimpleFeatureSerializer)],
                                    idSerializer: String => Array[Byte]) extends WritableFeature {

    private lazy val visibility = {
      val vis = feature.getUserData.get(FEATURE_VISIBILITY).asInstanceOf[String]
      if (vis == null) { EmptyBytes } else { vis.getBytes(StandardCharsets.UTF_8) }
    }

    override lazy val id: Array[Byte] = idSerializer.apply(feature.getID)

    override lazy val values: Seq[KeyValue] = serializers.map { case (colFamily, serializer) =>
      KeyValue(colFamily, EmptyBytes, visibility, serializer.serialize(feature))
    }
  }

  /**
    * Writable feature for attribute-level visibilities
    *
    * @param feature simple feature
    * @param colFamily attribute vis col family
    * @param serializer serializer
    * @param idSerializer feature id serializer
    */
  class AttributeLevelWritableFeature(val feature: SimpleFeature,
                                      val colFamily: Array[Byte],
                                      serializer: SimpleFeatureSerializer,
                                      idSerializer: String => Array[Byte]) extends WritableFeature {

    private lazy val visibilities: Array[String] = {
      val count = feature.getFeatureType.getAttributeCount
      val userData = Option(feature.getUserData.get(FEATURE_VISIBILITY).asInstanceOf[String])
      val visibilities = userData.map(_.split(",")).getOrElse(Array.fill(count)(""))
      require(visibilities.length == count,
        s"Per-attribute visibilities do not match feature type ($count values expected): ${userData.getOrElse("")}")
      visibilities
    }

    lazy val indexGroups: Seq[(Array[Byte], Array[Byte])] =
      visibilities.zipWithIndex.groupBy(_._1).map { case (vis, indices) =>
        (vis.getBytes(StandardCharsets.UTF_8), indices.map(_._2.toByte).sorted)
      }.toSeq

    override lazy val values: Seq[KeyValue] = indexGroups.map { case (vis, indices) =>
      val sf = new ScalaSimpleFeature(feature.getFeatureType, "")
      indices.foreach(i => sf.setAttribute(i, feature.getAttribute(i)))
      KeyValue(colFamily, indices, vis, serializer.serialize(sf))
    }

    override lazy val id: Array[Byte] = idSerializer.apply(feature.getID)
  }
}
