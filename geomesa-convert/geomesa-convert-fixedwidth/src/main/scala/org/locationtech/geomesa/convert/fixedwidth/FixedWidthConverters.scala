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

package org.locationtech.geomesa.convert.fixedwidth

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

case class FixedWidthField(name: String, transform: Transformers.Expr, s: Int, w: Int) extends Field {
  private val e = s + w
  override def eval(args: Any*)(implicit ec: EvaluationContext): Any = {
    transform.eval(args(0).asInstanceOf[String].substring(s, e))
  }
}

class FixedWidthConverterFactory extends SimpleFeatureConverterFactory[String] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "fixed-width")

  def buildConverter(targetSFT: SimpleFeatureType, conf: Config): FixedWidthConverter = {
    val fields    = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))
    new FixedWidthConverter(targetSFT, idBuilder, fields)
  }

  override def buildFields(fields: Seq[Config]): IndexedSeq[Field] = {
    fields.map { f =>
      val name = f.getString("name")
      val transform = Transformers.parseTransform(f.getString("transform"))
      if(f.hasPath("start") && f.hasPath("width")) {
        val s = f.getInt("start")
        val w = f.getInt("width")
        FixedWidthField(name, transform, s, w)
      } else SimpleField(name, transform)
    }.toIndexedSeq

  }
}

class FixedWidthConverter(val targetSFT: SimpleFeatureType,
                          val idBuilder: Transformers.Expr,
                          val inputFields: IndexedSeq[Field])
  extends ToSimpleFeatureConverter[String] {

  override def fromInputType(i: String): Array[Any] = Array(i)

}
