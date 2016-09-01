/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.function


import com.jayway.jsonpath.{Configuration, JsonPath}
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._
import com.jayway.jsonpath.Option.{ALWAYS_RETURN_LIST, DEFAULT_PATH_LEAF_TO_NULL, SUPPRESS_EXCEPTIONS}

class JsonPathFunction extends FunctionExpressionImpl(JsonPathFunction.PathName) {
  override def evaluate(o: AnyRef): AnyRef = {
    val json = getExpression(0).evaluate(o)
    if (json == null) {
      null
    } else {
      val path = getExpression(1).evaluate(null, classOf[String])
      val result: java.util.List[AnyRef] = JsonPath.using(JsonPathFunction.Config).parse(json.toString).read(path)
      if (result == null || result.isEmpty) null else if (result.size == 1) result.get(0) else result
    }
  }
}

class JsonExistsFunction extends FunctionExpressionImpl(JsonPathFunction.ExistsName) {
  override def evaluate(o: AnyRef): java.lang.Boolean = {
    val json = getExpression(0).evaluate(o)
    if (json == null) {
      java.lang.Boolean.FALSE
    } else {
      val path = getExpression(1).evaluate(null, classOf[String])
      val result: java.util.List[AnyRef] = JsonPath.using(JsonPathFunction.Config).parse(json.toString).read(path)
      result != null && !result.isEmpty
    }
  }
}

object JsonPathFunction {
  val PathName = new FunctionNameImpl("jsonPath", classOf[AnyRef],
    parameter("json", classOf[String]), parameter("path", classOf[String]))
  val ExistsName = new FunctionNameImpl("jsonExists", classOf[java.lang.Boolean],
    parameter("json", classOf[String]), parameter("path", classOf[String]))
  val Config = Configuration.builder.options(ALWAYS_RETURN_LIST, DEFAULT_PATH_LEAF_TO_NULL, SUPPRESS_EXCEPTIONS).build()
}
