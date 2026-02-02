/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.index.{ConfiguredIndex, DefaultFeatureIndexFactory, EmptyIndex, NamedIndex}
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs

/**
  * Factory for feature index implementations
  */
trait GeoMesaFeatureIndexFactory {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  /**
   * Gets all indices supported by this factory
   *
   * @return
   */
  def all(): Seq[NamedIndex]

  /**
   * Gets indices based on a type-level index flag
   *
   * @param sft simple feature type
   * @param flag index flag
   * @return
   */
  def fromFeatureFlag(sft: SimpleFeatureType, flag: String): Seq[IndexId]

  /**
   * Gets an index based on an attribute-level index flag
   *
   * @param sft simple feature type
   * @param descriptor attribute
   * @param flag index flag
   * @return
   */
  def fromAttributeFlag(sft: SimpleFeatureType, descriptor: AttributeDescriptor, flag: String): Seq[IndexId]

  /**
    * Gets the names and versions of available indices, based on the schema attributes. Can be used with
    * 'geomesa.indices.enabled' to specify default indices of the given type
    *
    * @param sft simple feature type
    * @return
    */
  @deprecated("Replaced with all()")
  def available(sft: SimpleFeatureType): Seq[(String, Int)] = all().map(i => (i.name, i.version))

  /**
   * Default indices for a feature type, based on user data hints and the schema attributes
   *
   * @param sft simple feature type
   * @return
   */
  @deprecated("Replaced with fromIndexFlag and fromAttributeFlag")
  def indices(sft: SimpleFeatureType, hint: Option[String]): Seq[IndexId] = {
    hint match {
      case Some(h) => fromFeatureFlag(sft, h)
      case None =>
        sft.getAttributeDescriptors.asScala.toSeq.flatMap { d =>
          val indices = d.getIndexFlags.flatMap(fromAttributeFlag(sft, d, _))
          if (indices.isEmpty && sft.getGeomField == d.getLocalName) {
            // add in default indices on geom
            all().collect { case i: ConfiguredIndex => i.indexFor(sft, d) }.flatten
          } else {
            indices.filterNot(_.name == EmptyIndex.name)
          }
        }
    }
  }

  /**
    * Create an index instance
    *
    * @param ds data store
    * @param sft simple feature type
    * @param index index identifier
    * @tparam T index filter values binding
    * @tparam U index key binding
    * @return
    */
  def create[T, U](ds: GeoMesaDataStore[_], sft: SimpleFeatureType, index: IndexId): Option[GeoMesaFeatureIndex[T, U]]
}

object GeoMesaFeatureIndexFactory extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  private val factories = ServiceLoader.load[GeoMesaFeatureIndexFactory]() :+ DefaultFeatureIndexFactory

  /**
    * Indices for a feature type, based on user data hints and the schema attributes
    *
    * @param sft simple feature type
    * @return
    */
  def indices(sft: SimpleFeatureType): Seq[IndexId] = {
    val enabled = fromIndexFlags(sft)
    if (enabled.nonEmpty) {
      enabled
    } else {
      // add in the ID index since there's no attribute descriptor for it
      fromAttributeFlags(sft) ++ IdIndex.defaultIndicesFor(sft)
    }
  }

  /**
   * Configures indices based on the EnabledIndices feature-level user data
   *
   * @param sft simple feature type
   * @return
   */
  private def fromIndexFlags(sft: SimpleFeatureType): Seq[IndexId] = {
    Option(sft.getUserData.get(Configs.EnabledIndices)).toSeq.flatMap(_.asInstanceOf[String].split(",").map(_.trim)).flatMap { flag =>
      val indices = factories.flatMap(_.fromFeatureFlag(sft, flag))
      if (indices.isEmpty) {
        throw new IllegalArgumentException(
          s"Invalid index flag '$flag' does not exist or does not support the schema: ${SimpleFeatureTypes.encodeType(sft)}")
      }
      indices
    }
  }

  /**
   * Configures indices based on attribute-level 'index' user data
   *
   * @param sft simple feature type
   * @return
   */
  private def fromAttributeFlags(sft: SimpleFeatureType): Seq[IndexId] = {
    sft.getAttributeDescriptors.asScala.toSeq.flatMap { d =>
      val indices = d.getIndexFlags.flatMap { flag =>
        val indices = factories.flatMap(_.fromAttributeFlag(sft, d, flag))
        if (indices.isEmpty) {
          throw new IllegalArgumentException(
            s"Invalid index flag '$flag' on attribute ${d.getLocalName} does not exist or does not support the type: " +
              d.getType.getBinding.getSimpleName)
        }
        indices
      }
      if (indices.isEmpty && sft.getGeomField == d.getLocalName) {
        // add in default indices on geom
        Seq(Z3Index, XZ3Index, Z2Index, XZ2Index).flatMap(i => i.indexFor(sft, d))
      } else {
        indices.filterNot(_.name == EmptyIndex.name)
      }
    }
  }

  /**
    * Gets the names of available indices, based on the schema attributes. Can be used with 'geomesa.indices.enabled'
    * to specify default indices of the given type
    *
    * @param sft simple feature type
    * @return
    */
  @deprecated("Replaced with all()")
  def available(sft: SimpleFeatureType): Seq[(String, Int)] = factories.flatMap(_.available(sft)).distinct

  /**
   * Gets all available indices
   *
   * @return
   */
  def all(): Seq[NamedIndex] = factories.flatMap(_.all())

  /**
    * Create an index instance
    *
    * @param ds data store
    * @param sft simple feature type
    * @param indices index identifiers
    * @return
    */
  def create(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, indices: Seq[IndexId]): Seq[GeoMesaFeatureIndex[_, _]] = {
    indices.map { index =>
      factories.flatMap(_.create(ds, sft, index).toSeq) match {
        case Seq(i) => i
        case Nil    => throw new IllegalArgumentException(s"No index found using identifier '$index'")
        case _      => throw new IllegalArgumentException(s"Multiple indices found using identifier '$index'")
      }
    }
  }
}
