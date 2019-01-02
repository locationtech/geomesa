/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.Caffeine
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureSerializer}
import org.locationtech.geomesa.index.metadata.CachedLazyMetadata
import org.locationtech.geomesa.index.planning.Transforms
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait ColumnGroups[T <: AnyRef] {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private val cache = {
    val expiry = CachedLazyMetadata.Expiry.toDuration.get.toMillis
    Caffeine.newBuilder().expireAfterWrite(expiry, TimeUnit.MILLISECONDS).build[String, Seq[(T, SimpleFeatureType)]]()
  }

  private lazy val defaultString = convert(default)
  private lazy val reservedStrings = reserved.map(convert)

  /**
    * Default column group, that contains all the attributes
    *
    * @return
    */
  def default: T

  /**
    * Any additional column groups that are used internally
    *
    * @return
    */
  protected def reserved: Set[T]

  /**
    * Convert a user-specified group into the appropriate type
    *
    * @param group group
    * @return
    */
  protected def convert(group: String): T

  /**
    * Convert a group back into a user-specified string
    *
    * @param group group
    * @return
    */
  protected def convert(group: T): String

  /**
    * Gets the column groups for a simple feature type. The default group will contain all columns
    *
    * @param sft simple feature type
    * @return
    */
  def apply(sft: SimpleFeatureType): Seq[(T, SimpleFeatureType)] = {
    val key = CacheKeyGenerator.cacheKey(sft)
    var groups = cache.getIfPresent(key)
    if (groups == null) {
      val map = scala.collection.mutable.Map.empty[String, SimpleFeatureTypeBuilder]

      sft.getAttributeDescriptors.asScala.foreach { descriptor =>
        descriptor.getColumnGroups().foreach { group =>
          map.getOrElseUpdate(group, new SimpleFeatureTypeBuilder()).add(descriptor)
        }
      }

      val sfts = map.map { case (group, builder) =>
        builder.setName(sft.getTypeName)
        val subset = builder.buildFeatureType()
        subset.getUserData.putAll(sft.getUserData)
        (convert(group), subset)
      } + (default -> sft)

      // return the smallest groups first
      groups = sfts.toSeq.sortBy(_._2.getAttributeCount)

      cache.put(key, groups)
    }
    groups
  }

  /**
    * Get serializers for each column group
    *
    * @param sft simple feature type
    * @return
    */
  def serializers(sft: SimpleFeatureType): Seq[(T, SimpleFeatureSerializer)] = {
    apply(sft).map { case (colFamily, subset) =>
      if (colFamily.eq(default)) {
        (colFamily, KryoFeatureSerializer(subset, SerializationOptions.withoutId))
      } else {
        (colFamily, new ProjectingKryoFeatureSerializer(sft, subset, SerializationOptions.withoutId))
      }
    }
  }

  /**
    * Find a column group that supports the given transform and filter
    *
    * @param sft simple feature type
    * @param transform transform definitions
    * @param ecql filter, if any
    * @return
    */
  def group(sft: SimpleFeatureType, transform: String, ecql: Option[Filter]): (T, SimpleFeatureType) = {
    val definitions = Transforms.definitions(transform)
    val groups = apply(sft).iterator
    var group = groups.next
    // last group has all the columns, so just return the last one if nothing else matches
    while (groups.hasNext && !Transforms.supports(group._2, definitions, ecql)) {
      group = groups.next
    }
    group
  }

  /**
    * Validate that the column groups do not overlap with reserved column groups
    *
    * @param sft simple feature type
    */
  def validate(sft: SimpleFeatureType): Unit = {
    // note: we validate against strings, as some col group types don't compare well (i.e. byte arrays)
    val groups = sft.getAttributeDescriptors.asScala.flatMap(_.getColumnGroups()).distinct
    groups.foreach { group =>
      if (group == defaultString || reservedStrings.contains(group)) {
        throw new IllegalArgumentException(s"Column group '$group' is reserved for internal use - " +
            "please choose another name")
      }
    }
  }
}
