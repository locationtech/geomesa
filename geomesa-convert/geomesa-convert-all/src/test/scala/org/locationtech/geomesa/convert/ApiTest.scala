/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
=======
>>>>>>> 5bf7fcb2bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a3aefef462 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5bf7fcb2bf (GEOMESA-3071 Move all converter state into evaluation context)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

<<<<<<< HEAD
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
=======
import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.TestConverterFactory.TestField
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicConfigConvert, BasicOptionsConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{Expression, TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.convert2.{AbstractConverter, AbstractConverterFactory, Field, SimpleFeatureConverter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import pureconfig.ConfigObjectCursor
import pureconfig.error.ConfigReaderFailures

@RunWith(classOf[JUnitRunner])
class ApiTest extends Specification {

  "SimpleFeatureConverters" should {
    "work with AbstractConverters that have not been updated to use the new API" in {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,line:String,dtg:Date,*geom:Point:srid=4326")
      val config =
        ConfigFactory.parseString(
          """{
            |  type = "test"
            |  fields = [
            |    { name = "name",  split = true, transform = "foo(bar($1))" }
            |    { name = "age",   split = true, transform = "$2::int"      }
            |    { name = "dtg",   split = true, transform = "dateTime($3)" }
            |    { name = "lon",   split = true, transform = "toDouble($4)" }
            |    { name = "lat",   split = true, transform = "toDouble($5)" }
            |    { name = "geom",  transform = "point($lon, $lat)"          }
            |    { name = "line",  transform = "bar(lineNo())"              }
            |  ]
            |}""".stripMargin
        )
      val data = "bob,21,2021-01-02T00:00:00.000Z,120,-45.5".getBytes(StandardCharsets.UTF_8)

      val converter = SimpleFeatureConverter(sft, config)
      converter must beAnInstanceOf[TestConverter]
      val result = WithClose(converter.process(new ByteArrayInputStream(data)))(_.toList)
      result must haveLength(1)
      result.head.getAttribute("name") mustEqual "foo bar bob"
      result.head.getAttribute("age") mustEqual 21
      result.head.getAttribute("line") mustEqual "bar 0"
      FastConverter.convert(result.head.getAttribute("dtg"), classOf[String]) mustEqual "2021-01-02T00:00:00.000Z"
      FastConverter.convert(result.head.getAttribute("geom"), classOf[String]) mustEqual "POINT (120 -45.5)"
    }
  }

}
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)

class TestConverter(sft: SimpleFeatureType, config: BasicConfig, fields: Seq[TestField], options: BasicOptions)
    extends AbstractConverter[String, BasicConfig, TestField, BasicOptions](sft, config, fields, options) {

  import scala.collection.JavaConverters._

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[String] =
    CloseableIterator(IOUtils.lineIterator(is, options.encoding).asScala, is.close())

  override protected def values(
      parsed: CloseableIterator[String],
      ec: EvaluationContext): CloseableIterator[Array[Any]] = parsed.map(Array[Any](_))
}

<<<<<<< HEAD
class TestConverterFactory extends AbstractConverterFactory[TestConverter, BasicConfig, TestField, BasicOptions](
  "test", BasicConfigConvert, TestConverterFactory.TestFieldConvert, BasicOptionsConvert)
=======
class TestConverterFactory extends AbstractConverterFactory[TestConverter, BasicConfig, TestField, BasicOptions] {
  override protected val typeToProcess: String = "test"
  override protected implicit val configConvert: ConverterConfigConvert[BasicConfig] = BasicConfigConvert
  override protected implicit val fieldConvert: FieldConvert[TestField] = TestConverterFactory.TestFieldConvert
  override protected implicit val optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert
}
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)

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

<<<<<<< HEAD
  case class TestDerivedField(name: String, transforms: Option[Expression]) extends TestField {
    override def fieldArg: Option[Array[AnyRef] => AnyRef] = None
  }

  case class TestSplitField(name: String, transforms: Option[Expression]) extends TestField {
    override def fieldArg: Option[Array[AnyRef] => AnyRef] =
      Some(args => Array[Any](args(0)) ++ args(0).asInstanceOf[String].split(","))
=======
  case class TestDerivedField(name: String, transforms: Option[Expression]) extends TestField

  case class TestSplitField(name: String, transforms: Option[Expression]) extends TestField {
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any =
      super.eval(Array[Any](args(0)) ++ args(0).asInstanceOf[String].split(","))
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
  }
}

class TestFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(foo, bar)

  private val foo = new NamedTransformerFunction(Seq("foo")) {
<<<<<<< HEAD
    override def apply(args: Array[AnyRef]): AnyRef = "foo " + args(0)
=======
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = "foo " + args(0)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
  }

  private val bar = new TransformerFunction {
    override def names: Seq[String] = Seq("bar")
<<<<<<< HEAD
    override def apply(args: Array[AnyRef]): AnyRef = "bar " + args(0)
    override def withContext(ec: EvaluationContext): TransformerFunction = this
=======
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = "bar " + args(0)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
  }
}
