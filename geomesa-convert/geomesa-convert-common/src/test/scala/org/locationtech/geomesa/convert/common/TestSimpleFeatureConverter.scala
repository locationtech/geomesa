/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.convert.common

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.immutable

class TestSimpleFeatureConverter(val targetSFT: SimpleFeatureType,
                                 val idBuilder: Expr,
                                 val inputFields: IndexedSeq[Field],
                                 val userDataBuilder: Map[String, Expr],
                                 val caches: Map[String, EnrichmentCache],
                                 val parseOpts: ConvertParseOpts) extends LinesToSimpleFeatureConverter {
  override def fromInputType(i: String, ec: EvaluationContext): Iterator[Array[Any]] =
    Iterator(i.split(",").asInstanceOf[Array[Any]])
}

class TestSimpleFeatureConverterFactory extends AbstractSimpleFeatureConverterFactory[String] {
  override protected def typeToProcess: String = "test"

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: immutable.IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        cacheServices: Map[String, EnrichmentCache],
                                        parseOpts: ConvertParseOpts): SimpleFeatureConverter[String] = {
    new TestSimpleFeatureConverter(sft, idBuilder, fields, userDataBuilder, cacheServices, parseOpts)
  }
}
