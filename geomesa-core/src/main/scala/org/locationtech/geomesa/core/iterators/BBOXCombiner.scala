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

package org.locationtech.geomesa.core.iterators

import java.util

import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.io.WKTReader
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.Combiner
import org.locationtech.geomesa.core.iterators.BBOXCombiner._
import org.locationtech.geomesa.utils.geohash.BoundingBox

import scala.collection.JavaConversions._

class BBOXCombiner extends Combiner {
  override def reduce(p1: Key, p2: util.Iterator[Value]): Value = {
    if(p2.hasNext) {
      bboxToValue(reduceValuesToBoundingBox(p2))
    } else {
      new Value()
    }
  }
}

object BBOXCombiner {
  val wktReader = new ThreadLocal[WKTReader] {
    override def initialValue = new WKTReader()
  }

  def reduceValuesToBoundingBox(values: util.Iterator[Value]): BoundingBox = {
    values.map(valueToBbox).reduce( (a, b) => BoundingBox.getCoveringBoundingBox(a, b) )
  }

  // These two functions are inverse
  def bboxToValue(bbox: BoundingBox): Value = {
    new Value((bbox.ll.toString + ":" + bbox.ur.toString).getBytes)
  }

  def valueToBbox(value: Value): BoundingBox = {
    val wkts = value.toString.split(":")
    val localReader = wktReader.get
    BoundingBox(localReader.read(wkts(0)).asInstanceOf[Point], localReader.read(wkts(1)).asInstanceOf[Point])
  }
}