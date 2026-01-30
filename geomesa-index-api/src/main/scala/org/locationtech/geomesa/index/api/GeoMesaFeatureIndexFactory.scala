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
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.{DefaultFeatureIndexFactory, EmptyIndex, NamedIndex}
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{AttributeOptions, Configs}

/**
  * Factory for feature index implementations
  */
trait GeoMesaFeatureIndexFactory {

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
  def fromIndexFlag(sft: SimpleFeatureType, flag: String): Seq[IndexId]

  /**
   * Gets an index based on an attribute-level index flag
   *
   * @param sft simple feature type
   * @param descriptor attribute
   * @param flag index flag
   * @return
   */
  def fromAttributeFlag(sft: SimpleFeatureType, descriptor: AttributeDescriptor, flag: String): Option[IndexId]

  /**
   * Gets default indices for an attribute
   *
   * @param sft simple feature type
   * @param descriptor attribute
   * @return
   */
  def defaults(sft: SimpleFeatureType, descriptor: AttributeDescriptor): Seq[IndexId]

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
  @deprecated("Replaced with fromIndexFlag, fromAttributeFlag and defaults")
  def indices(sft: SimpleFeatureType, hint: Option[String]): Seq[IndexId]

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
      fromAttributeFlags(sft) ++ IdIndex.defaults(sft).map(IndexId(IdIndex.name, IdIndex.version, _))
    }
  }

  /**
   * Configures indices based on the EnabledIndices feature-level user data
   *
   * @param sft simple feature type
   * @return
   */
  private def fromIndexFlags(sft: SimpleFeatureType): Seq[IndexId] = {
    splitFlag(sft.getUserData.get(Configs.EnabledIndices)).flatMap { flag =>
      val indices = factories.flatMap(_.fromIndexFlag(sft, flag))
      if (indices.isEmpty) {
        if (flag == AttributeIndex.name) {
          logger.warn(s"Found configured $flag index but no attributes are flagged for indexing")
        } else {
          throw new IllegalArgumentException(
            s"Invalid index flag '$flag' does not exist or does not support the schema: ${SimpleFeatureTypes.encodeType(sft)}")
        }
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
    sft.getAttributeDescriptors.asScala.flatMap { d =>
      val indices = splitFlag(d.getUserData.get(AttributeOptions.OptIndex)).flatMap { flag =>
        val indices = factories.flatMap(_.fromAttributeFlag(sft, d, flag))
        if (indices.isEmpty) {
          throw new IllegalArgumentException(
            s"Invalid index flag '$flag' on attribute ${d.getLocalName} does not exist or does not support the type: " +
              d.getType.getBinding.getSimpleName)
        }
        indices
      }
      if (indices.isEmpty) {
        factories.flatMap(_.defaults(sft, d))
      } else {
        indices.filterNot(_.name == EmptyIndex.name)
      }
    }
  }

  private def splitFlag(flag: AnyRef): Seq[String] = Option(flag).toSeq.flatMap(_.asInstanceOf[String].split(",").map(_.trim))

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
