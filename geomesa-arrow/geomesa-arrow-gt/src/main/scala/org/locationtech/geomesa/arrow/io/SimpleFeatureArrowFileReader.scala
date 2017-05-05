/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, InputStream}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.Field
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.io.reader.{CachingSimpleFeatureArrowFileReader, StreamingSimpleFeatureArrowFileReader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.{ArrowAttributeReader, ArrowDictionary, GeometryFields}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureSpecParser
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

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

  /**
    * Reads features from the underlying arrow file
    *
    * @param filter filter to apply
    * @return
    */
  def features(filter: Filter = Filter.INCLUDE): Iterator[ArrowSimpleFeature]
}

object SimpleFeatureArrowFileReader {

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

  private [io] def loadDictionaries(fields: Seq[Field],
                                    provider: DictionaryProvider,
                                    preLoad: () => Unit = () => {}): Map[String, ArrowDictionary] = {
    import scala.collection.JavaConversions._

    val tuples = fields.flatMap { field =>
      Option(field.getDictionary).toSeq.map { encoding =>
        preLoad()
        val vector = provider.lookup(encoding.getId).getVector
        val spec = SimpleFeatureSpecParser.parseAttribute(field.getName)
        val (objectType, bindings) = ObjectType.selectType(spec.clazz, spec.options)
        val isSingle = GeometryFields.precisionFromField(field) == FloatingPointPrecision.SINGLE
        val precision = if (isSingle) { GeometryPrecision.Float } else { GeometryPrecision.Double }
        val attributeReader = ArrowAttributeReader(bindings.+:(objectType), spec.clazz, vector, None, precision)

        val values = ArrayBuffer.empty[AnyRef]
        var i = 0
        while (i < vector.getAccessor.getValueCount) {
          values.append(attributeReader.apply(i))
          i += 1
        }
        field.getName -> new ArrowDictionary(values, encoding)
      }
    }
    tuples.toMap
  }
}
