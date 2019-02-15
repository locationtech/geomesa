/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.fixedwidth

import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.fixedwidth.FixedWidthConverter.FixedWidthField
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, Field}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeatureType

class FixedWidthConverter(sft: SimpleFeatureType,
                          config: BasicConfig,
                          fields: Seq[FixedWidthField],
                          options: BasicOptions)
    extends AbstractConverter[String, BasicConfig, FixedWidthField, BasicOptions](sft, config, fields, options) {

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[String] = {
    new CloseableIterator[String] {
      private val lines = IOUtils.lineIterator(is, options.encoding)

      override def hasNext: Boolean = lines.hasNext

      override def next(): String = {
        ec.counter.incLineCount()
        lines.next
      }

      override def close(): Unit = lines.close()
    }
  }

  override protected def values(parsed: CloseableIterator[String],
                                ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    val array = Array.ofDim[Any](1)
    parsed.map { line =>
      array(0) = line
      array
    }
  }
}

object FixedWidthConverter {

  sealed trait FixedWidthField extends Field

  case class OffsetField(name: String, transforms: Option[Expression], start: Int, width: Int)
      extends FixedWidthField {
    private val endIdx: Int = start + width
    private val mutableArray = Array.ofDim[Any](1)
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
      mutableArray(0) = args(0).asInstanceOf[String].substring(start, endIdx)
      super.eval(mutableArray)
    }
  }

  case class DerivedField(name: String, transforms: Option[Expression]) extends FixedWidthField
}
