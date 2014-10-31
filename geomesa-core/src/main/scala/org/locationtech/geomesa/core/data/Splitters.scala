/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.core.data

import java.util

import org.apache.hadoop.io.Text
import org.locationtech.geomesa.data.TableSplitter

import scala.collection.JavaConversions._

class DigitSplitter extends TableSplitter {
  /**
   * @param options allowed options are "fmt", "min", and "max"
   * @return
   */
  override def getSplits(options: util.Map[String, String]): Array[Text] = {
    val fmt = options.getOrElse("fmt", "%01d")
    val min = options.getOrElse("min", "0").toInt
    val max = options.getOrElse("max", "0").toInt
    (min to max).map(fmt.format(_)).map(s => new Text(s)).toArray
  }
}

class HexSplitter extends TableSplitter {
  val hexSplits = "0123456789abcdefABCDEF".map(_.toString).map(new Text(_)).toArray
  override def getSplits(options: util.Map[String, String]): Array[Text] = hexSplits
}

class AlphaNumericSplitter extends TableSplitter {
  override def getSplits(options: util.Map[String, String]): Array[Text] =
    (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).map(c => new Text("" + c)).toArray
}
