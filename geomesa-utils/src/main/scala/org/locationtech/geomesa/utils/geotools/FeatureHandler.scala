/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.utils.geotools

import java.io.{BufferedWriter, File, FileWriter}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.{Base64, DataUtilities}
import org.geotools.feature.FeatureIterator
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.identity.FeatureId

import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: jnh5y
 * Date: 2/8/13
 * Time: 4:50 PM
 */
object FeatureHandler {
  val OUTPUT_FIELD_SEPARATOR : String = "|"
  val OUTPUT_FIELD_SEPARATOR_CHAR : Char = OUTPUT_FIELD_SEPARATOR.charAt(0)

  def features2csv(featureIterator: FeatureIterator[SimpleFeature],
                   newType: SimpleFeatureType, outputFile: String): List[FeatureId] = {
    val fout = new BufferedWriter(new FileWriter(new File(outputFile)))
    val buffer = new ListBuffer[FeatureId]
    while (featureIterator.hasNext) {
      val sf = featureIterator.next()

      try {
        var geom = sf.getDefaultGeometry.asInstanceOf[Geometry]
        if (geom != null) { // Otherwise we didn't read the line right.  We'll ignore it then.
          if (!geom.isValid) {  // If Vivid says our geometry is valid, we can buffer it.
            geom = geom.buffer(0)
            if (!geom.isValid) throw new Exception("Invalid geometry:\n" + geom)  // Really, really invalid.
          }
          if (geom.isEmpty) throw new Exception("Empty geometry.")

          val wkb64 = Base64.encodeBytes(WKBUtils.write(geom), Base64.DONT_BREAK_LINES)
          if (wkb64.length < 1) throw new Exception("Invalid base-64 encoding")

          // having passed the pre-requisites, write out this line
          val encodedFeature = DataUtilities.encodeFeature(sf)
          fout.write(List(wkb64, encodedFeature).mkString(OUTPUT_FIELD_SEPARATOR))
          fout.newLine()

          buffer += sf.getIdentifier
        }
        else {
          throw new Exception("Read a null geometry")
        }
      } catch {
        //@TODO this would be a good place for logging
        case e:Exception => System.err.println("[WARNING] Problem reading geometry or attributes from Shapefile.\n  " +
          "Geometry:  " + sf.getDefaultGeometry + "\n  Attributes:  " + sf.getAttributes)
      }
    }
    fout.flush()
    fout.close()

    buffer.toList
  }
}