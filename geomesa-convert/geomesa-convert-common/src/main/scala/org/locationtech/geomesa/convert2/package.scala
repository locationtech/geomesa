/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import java.nio.charset.Charset

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.Modes.ParseMode
import org.locationtech.geomesa.convert.{EvaluationContext, SimpleFeatureValidator}
import org.locationtech.geomesa.convert2.transforms.Expression

package object convert2 {

  trait ConverterConfig {
    def `type`: String
    def idField: Option[Expression]
    def caches: Map[String, Config]
    def userData: Map[String, Expression]
  }

  trait Field {
    def name: String
    def transforms: Option[Expression]
    def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = transforms.map(_.eval(args)).getOrElse(args(0))
  }

  trait ConverterOptions {
    def validators: SimpleFeatureValidator
    def parseMode: ParseMode
    def errorMode: ErrorMode
    def encoding: Charset
    def verbose: Boolean
  }
}
