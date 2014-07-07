/*
 *
 *  * Copyright 2014 Commonwealth Computer Research, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the License);
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an AS IS BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package geomesa.core.integration.data

import java.text.SimpleDateFormat

import geomesa.core.index.Constants
import org.geotools.data.DataUtilities
import org.opengis.feature.simple.SimpleFeatureType
import scala.collection.JavaConverters._

import scala.io.Source

class DataType(typeString: String) {

  private val dataUrl = getClass.getClassLoader.getResource(s"$typeString.tsv")

  if (dataUrl == null) {
    throw new RuntimeException(
      s"Can't load data type resource $typeString.tsv. Please ensure the data file is available on the classpath.")
  }

  private val attributeList: List[String] = {
    val source = getSource
    val header = getSource.getLines().next()
    source.close()
    header.split("\t").toList
  }

  // non-id attributes (id is first)
  val attributes: String = attributeList.drop(1).mkString(",")

  val idAttribute: String = attributeList(0)
  // for simplicity, date always needs to be in 'dtg' attribute
  val dateAttribute: String = "dtg"

  val sftName: String = typeString
  val table: String = typeString

  val simpleFeatureType: SimpleFeatureType = {
    val featureType = DataUtilities.createType(sftName, attributes)
    featureType.getUserData.put(Constants.SF_PROPERTY_START_TIME, dateAttribute)
    featureType
  }

  def getSource: Source = Source.fromURL(dataUrl)
}
