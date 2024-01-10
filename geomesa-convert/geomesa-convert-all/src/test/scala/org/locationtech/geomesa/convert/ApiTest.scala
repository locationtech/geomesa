/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import org.apache.commons.io.IOUtils
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.TestConverterFactory.TestField
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicConfigConvert, BasicOptionsConvert, FieldConvert, OptionConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{Expression, TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.convert2.{AbstractConverter, AbstractConverterFactory, Field}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import pureconfig.ConfigObjectCursor
import pureconfig.error.ConfigReaderFailures

import java.io.InputStream

class TestConverter(sft: SimpleFeatureType, config: BasicConfig, fields: Seq[TestField], options: BasicOptions)
    extends AbstractConverter[String, BasicConfig, TestField, BasicOptions](sft, config, fields, options) {

  import scala.collection.JavaConverters._

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[String] =
    CloseableIterator(IOUtils.lineIterator(is, options.encoding).asScala, is.close())

  override protected def values(
      parsed: CloseableIterator[String],
      ec: EvaluationContext): CloseableIterator[Array[Any]] = parsed.map(Array[Any](_))
}

class TestConverterFactory extends AbstractConverterFactory[TestConverter, BasicConfig, TestField, BasicOptions](
  "test", BasicConfigConvert, TestConverterFactory.TestFieldConvert, BasicOptionsConvert)

object TestConverterFactory {

  object TestFieldConvert extends FieldConvert[TestField] with OptionConvert {

    override protected def decodeField(cur: ConfigObjectCursor,
        name: String,
        transform: Option[Expression]): Either[ConfigReaderFailures, TestField] = {
      val split = cur.atKeyOrUndefined("split")
      if (split.isUndefined) { Right(TestDerivedField(name, transform)) } else {
        for { s <- PrimitiveConvert.booleanConfigReader.from(split).right } yield {
          if (s) { TestSplitField(name, transform) } else { TestDerivedField(name, transform) }
        }
      }
    }

    override protected def encodeField(field: TestField, base: java.util.Map[String, AnyRef]): Unit = {
      field match {
        case _: TestSplitField => base.put("split", java.lang.Boolean.TRUE)
        case _ =>
      }
    }
  }

  trait TestField extends Field

  case class TestDerivedField(name: String, transforms: Option[Expression]) extends TestField {
    override def fieldArg: Option[Array[AnyRef] => AnyRef] = None
  }

  case class TestSplitField(name: String, transforms: Option[Expression]) extends TestField {
    override def fieldArg: Option[Array[AnyRef] => AnyRef] =
      Some(args => Array[Any](args(0)) ++ args(0).asInstanceOf[String].split(","))
  }
}

class TestFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(foo, bar)

  private val foo = new NamedTransformerFunction(Seq("foo")) {
    override def apply(args: Array[AnyRef]): AnyRef = "foo " + args(0)
  }

  private val bar = new TransformerFunction {
    override def names: Seq[String] = Seq("bar")
    override def apply(args: Array[AnyRef]): AnyRef = "bar " + args(0)
    override def withContext(ec: EvaluationContext): TransformerFunction = this
  }
}
