/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.TestConverterFactory.TestField
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicConfigConvert, BasicOptionsConvert, FieldConvert, OptionConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{Expression, TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.convert2.{AbstractConverter, AbstractConverterFactory, Field}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeatureType
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
