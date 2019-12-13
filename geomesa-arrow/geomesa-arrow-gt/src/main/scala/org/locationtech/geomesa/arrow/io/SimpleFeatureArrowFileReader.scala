/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, InputStream}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.types.pojo.Field
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.filter.ArrowFilterOptimizer
import org.locationtech.geomesa.arrow.io.reader.{CachingSimpleFeatureArrowFileReader, StreamingSimpleFeatureArrowFileReader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.{DescriptorKey, SimpleFeatureEncoding}
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * For reading simple features from an arrow file written by SimpleFeatureArrowFileWriter.
  *
  * Expects arrow streaming format (no footer). Can handle multiple 'files' in a single input stream
  */
trait SimpleFeatureArrowFileReader extends Closeable {

  /**
    * The simple feature type for the file. Note: this may change as features are read,
    * if there are multiple logical 'files' in the input stream. By convention, we keep
    * a single file with a single sft, but that is not enforced.
    *
    * @return current simple feature type
    */
  def sft: SimpleFeatureType

  /**
    * Dictionaries from the file. Note: this may change as features are read, if there are
    * multiple logical 'files' in the input stream. This method is exposed for completeness,
    * but generally would not be needed since dictionary values are automatically decoded
    * into the returned simple features.
    *
    * @return current dictionaries, keyed by attribute
    */
  def dictionaries: Map[String, ArrowDictionary]

  def vectors: Seq[SimpleFeatureVector]

  /**
    * Reads features from the underlying arrow file
    *
    * @param filter filter to apply
    * @return
    */
  def features(filter: Filter = Filter.INCLUDE): Iterator[ArrowSimpleFeature] with Closeable
}

object SimpleFeatureArrowFileReader {

  type VectorToIterator = SimpleFeatureVector => Iterator[ArrowSimpleFeature]

  /**
    * A reader that caches results in memory. Repeated calls to `features()` will not require re-reading
    * the input stream. Returned features will be valid until `close()` is called
    *
    * @param is input stream
    * @param allocator buffer allocator
    * @return
    */
  def caching(is: InputStream)(implicit allocator: BufferAllocator): SimpleFeatureArrowFileReader =
    new CachingSimpleFeatureArrowFileReader(is)

  /**
    * A reader that streams results. Repeated calls to `features()` will re-read the input stream. Returned
    * features may not be valid after a call to `next()`, as the underlying data may be reclaimed.
    *
    * @param is creates a new input stream for reading
    * @param allocator buffer allocator
    * @return
    */
  def streaming(is: () => InputStream)(implicit allocator: BufferAllocator): SimpleFeatureArrowFileReader =
    new StreamingSimpleFeatureArrowFileReader(is)

  /**
    *
    * @param fields dictionary encoded fields
    * @param provider dictionary provider
    * @return
    */
  private [io] def loadDictionaries(fields: Seq[Field],
                                    provider: DictionaryProvider,
                                    precision: SimpleFeatureEncoding): Map[String, ArrowDictionary] = {
    fields.flatMap { field =>
      Option(field.getDictionary).toSeq.map { encoding =>
        val descriptor = SimpleFeatureTypes.createDescriptor(field.getMetadata.get(DescriptorKey))
        val vector = provider.lookup(encoding.getId).getVector
        field.getName -> ArrowDictionary.create(encoding, vector, descriptor, precision)
      }
    }.toMap
  }

  /**
    * Reads features from simple feature vectors based on a filter
    *
    * @param sft simple feature type
    * @param filter filter
    * @param skip indicator that we should skip any further batches
    * @param sort sort for the file being read, if any
    * @param dictionaries dictionaries
    * @return
    */
  private [io] def features(sft: SimpleFeatureType,
                            filter: Filter,
                            skip: SkipIndicator,
                            sort: Option[(String, Boolean)],
                            dictionaries: Map[String, ArrowDictionary]): VectorToIterator = {
    val optimized = ArrowFilterOptimizer.rewrite(filter, sft, dictionaries)
    sort match {
      case None => features(_, optimized)
      case Some((field, reverse)) =>
        val i = sft.indexOf(field)
        val binding = sft.getDescriptor(i).getType.getBinding
        val bounds = FilterHelper.extractAttributeBounds(filter, field, binding).values
        if (bounds.isEmpty) {
          features(_, optimized)
        } else {
          sortedFeatures(_, optimized, skip, bounds.asInstanceOf[Seq[Bounds[Comparable[Any]]]], i, reverse)
        }
    }
  }

  /**
    * Reads features from a simple feature vector
    *
    * @param vector simple feature vector
    * @param filter filter
    * @return
    */
  private def features(vector: SimpleFeatureVector, filter: Filter): Iterator[ArrowSimpleFeature] = {
    val total = vector.reader.getValueCount
    if (total == 0) { Iterator.empty } else {
      // re-use the same feature object
      val feature = vector.reader.feature
      val all = Iterator.range(0, total).map { i => vector.reader.load(i); feature }
      if (filter == Filter.INCLUDE) { all } else {
        all.filter(filter.evaluate)
      }
    }
  }

  /**
    * Reads features from a simple feature vector. The underlying features are assumed to be sorted
    *
    * @param vector simple feature vector
    * @param filter filter
    * @param skip will be toggled if no further vectors need to be queried due to sort order and filter bounds
    * @param filterBounds bounds for the sort field, extracted from the filter
    * @param sortField field that the features are sorted by
    * @param reverse if the sort order is reversed or not
    * @return
    */
  private def sortedFeatures(vector: SimpleFeatureVector,
                             filter: Filter,
                             skip: SkipIndicator,
                             filterBounds: Seq[Bounds[Comparable[Any]]],
                             sortField: Int,
                             reverse: Boolean): Iterator[ArrowSimpleFeature] = {
    val total = vector.reader.getValueCount

    if (total == 0 || skip.skip) { Iterator.empty } else {
      // re-use the same feature object
      val feature = vector.reader.feature

      // bounds for the current batch
      val currentBatchBounds = {
        vector.reader.load(0)
        val lo = Bound(Option(feature.getAttribute(sortField).asInstanceOf[Comparable[Any]]), inclusive = true)
        vector.reader.load(total - 1)
        val hi = Bound(Option(feature.getAttribute(sortField).asInstanceOf[Comparable[Any]]), inclusive = true)
        if (reverse) { Bounds(hi, lo) } else { Bounds(lo, hi) }
      }

      if (filterBounds.exists(Bounds.intersection(_, currentBatchBounds).isDefined)) {
        // we have a match in this batch
        val all = Iterator.range(0, vector.reader.getValueCount).map { i => vector.reader.load(i); feature }
        all.filter(filter.evaluate)
      } else {
        // nothing from this batch matches, check to see if any further batches could match
        val hasMore = if (reverse) {
          filterBounds.exists(fb => Bounds.smallerLowerBound(fb.lower, currentBatchBounds.lower).eq(fb.lower))
        } else {
          filterBounds.exists(fb => Bounds.largerUpperBound(fb.upper, currentBatchBounds.upper).eq(fb.upper))
        }
        // toggle the skip indicator if there are no further batches that could match
        skip.skip = !hasMore
        Iterator.empty
      }
    }
  }

  // holder for a skip indicator - this will be toggled if we ever determine there can be no more results
  private [io] class SkipIndicator(var skip: Boolean = false)
}
