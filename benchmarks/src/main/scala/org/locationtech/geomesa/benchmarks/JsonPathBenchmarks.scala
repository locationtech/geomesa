/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.benchmarks
import java.util.concurrent.TimeUnit

import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.benchmarks.JsonPathBenchmarks.{AttributeState, FunctionState}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.openjdk.jmh.annotations._

@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
class JsonPathBenchmarks {

  // to run: java -jar benchmarks/target/jmh-benchmarks.jar

  @Benchmark
  def jsonPathFunction(state: FunctionState): Unit = {
    state.function.evaluate(state.features.head)
    state.function.evaluate(state.features.last)
  }

  @Benchmark
  def jsonPathProperty(state: AttributeState): Unit = {
    state.attribute.evaluate(state.features.head)
    state.attribute.evaluate(state.features.last)
  }
}

object JsonPathBenchmarks {

  class FeatureState {
    val sft: SimpleFeatureType = SimpleFeatureTypes.createType("foo", "properties:String:json=true")

    val features = Seq(
      ScalaSimpleFeature.create(sft, "0", """{"bar": 0, "foo": "1234", "baz":"-1"}"""),
      ScalaSimpleFeature.create(sft, "1", """{"bar": 0, "foo": "1235", "baz":"-1"}""")
    )
  }

  @State(Scope.Benchmark)
  class FunctionState extends FeatureState {
    val function: Filter = ECQL.toFilter("jsonPath('$.properties.foo') = '1234'")
  }

  @State(Scope.Benchmark)
  class AttributeState extends FeatureState {
    val attribute: Filter = ECQL.toFilter("\"$.properties.foo\" = '1234'")
  }
}
