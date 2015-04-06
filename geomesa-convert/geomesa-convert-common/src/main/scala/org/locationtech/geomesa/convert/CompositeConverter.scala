/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.convert

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Predicate}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

class CompositeConverterFactory[I] extends SimpleFeatureConverterFactory[I] {
  override def canProcess(conf: Config): Boolean = canProcessType(conf, "composite-converter")

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I] = {
    val converters: Seq[(Predicate, SimpleFeatureConverter[I])] =
      conf.getConfigList("converters").map { c =>
        val pred = Transformers.parsePred(c.getString("predicate"))
        val converter = SimpleFeatureConverters.build[I](sft, conf.getConfig(c.getString("converter")))
        (pred, converter)
      }
    new CompositeConverter[I](sft, converters)
  }

}

class CompositeConverter[I](val targetSFT: SimpleFeatureType,
                            converters: Seq[(Predicate, SimpleFeatureConverter[I])])
  extends SimpleFeatureConverter[I] {

  override def processInput(is: Iterator[I],  gParams: Map[String, String] = Map.empty): Iterator[SimpleFeature] = {
    is.flatMap { input =>
      converters.view.flatMap { case (pred, conv) =>
        processIfValid(input, pred, conv, gParams)
      }.headOption
    }
  }

  // noop
  override def processSingleInput(i: I, gParams: Map[String, String] = Map.empty): Option[SimpleFeature] = null

  private val mutableArray = Array.ofDim[Any](1)

  // to satisfy pred.eval() implicit evaluation context requirement
  implicit val emptyEC = new EvaluationContext(mutable.HashMap.empty[String, Int], Array.empty[Any])

  def processIfValid(input: I, pred: Predicate, conv: SimpleFeatureConverter[I], gParams: Map[String, String]) = {
    val opt =
      Try {
        mutableArray(0) = input
        pred.eval(mutableArray)
      }.toOption

    opt.flatMap { v => if (v) conv.processSingleInput(input, gParams) else None }
  }
}
