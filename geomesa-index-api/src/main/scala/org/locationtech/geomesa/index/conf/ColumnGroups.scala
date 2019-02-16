/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.Caffeine
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureSerializer}
import org.locationtech.geomesa.index.metadata.CachedLazyMetadata
import org.locationtech.geomesa.index.planning.Transforms
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class ColumnGroups {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  /**
    * Gets the column groups for a simple feature type. The default group will contain all columns
    *
    * @param sft simple feature type
    * @return
    */
  def apply(sft: SimpleFeatureType): Seq[(Array[Byte], SimpleFeatureType)] = {
    val key = CacheKeyGenerator.cacheKey(sft)
    var groups = ColumnGroups.cache.getIfPresent(key)
    if (groups == null) {
      if (sft.getVisibilityLevel == VisibilityLevel.Attribute) {
        groups = IndexedSeq((ColumnGroups.Attributes, sft))
      } else {
        val map = scala.collection.mutable.Map.empty[String, SimpleFeatureTypeBuilder]

        sft.getAttributeDescriptors.asScala.foreach { descriptor =>
          descriptor.getColumnGroups().foreach { group =>
            map.getOrElseUpdate(group, new SimpleFeatureTypeBuilder()).add(descriptor)
          }
        }

        val sfts = map.map { case (group, builder) =>
          builder.setName(sft.getTypeName)
          val subset = SimpleFeatureTypes.immutable(builder.buildFeatureType(), sft.getUserData)
          (group.getBytes(StandardCharsets.UTF_8), subset)
        } + (ColumnGroups.Default -> sft)

        // return the smallest groups first, for consistency tiebreaker is string comparison of group
        groups = sfts.toIndexedSeq.sortBy { case (group, subset) =>
          (subset.getAttributeCount, new String(group, StandardCharsets.UTF_8))
        }
      }

      ColumnGroups.cache.put(key, groups)
    }
    groups
  }

  /**
    * Get serializers for each column group
    *
    * @param sft simple feature type
    * @return
    */
  def serializers(sft: SimpleFeatureType): Seq[(Array[Byte], SimpleFeatureSerializer)] = {
    apply(sft).map { case (colFamily, subset) =>
      if (colFamily.eq(ColumnGroups.Default) || colFamily.eq(ColumnGroups.Attributes)) {
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
  def group(sft: SimpleFeatureType, transform: Option[String], ecql: Option[Filter]): (Array[Byte], SimpleFeatureType) = {
    val groups = apply(sft)
    transform.map(Transforms.definitions) match {
      case None => groups.last
      case Some(definitions) =>
        val iter = groups.iterator
        var group = iter.next
        // last group has all the columns, so just return the last one if nothing else matches
        while (iter.hasNext && !Transforms.supports(group._2, definitions, ecql)) {
          group = iter.next
        }
        group
    }
  }

  /**
    * Validate that the column groups do not overlap with reserved column groups
    *
    * @param sft simple feature type
    */
  def validate(sft: SimpleFeatureType): Unit = {
    val groups = sft.getAttributeDescriptors.asScala.flatMap(_.getColumnGroups()).distinct
    groups.foreach { group =>
      if (group == ColumnGroups.DefaultString || group == ColumnGroups.AttributesString) {
        throw new IllegalArgumentException(s"Column group '$group' is reserved for internal use - " +
            "please choose another name")
      }
    }
    if (sft.getVisibilityLevel == VisibilityLevel.Attribute && groups.nonEmpty) {
      throw new IllegalArgumentException("Column groups are not supported when using attribute-level visibility")
    }
  }
}

object ColumnGroups {

  private val DefaultString = "d"
  private val AttributesString = "a"

  val Default: Array[Byte] = DefaultString.getBytes(StandardCharsets.UTF_8)
  val Attributes: Array[Byte] = AttributesString.getBytes(StandardCharsets.UTF_8)

  private val cache =
    Caffeine.newBuilder()
        .expireAfterWrite(CachedLazyMetadata.Expiry.toDuration.get.toMillis, TimeUnit.MILLISECONDS)
        .build[String, IndexedSeq[(Array[Byte], SimpleFeatureType)]]()
}
