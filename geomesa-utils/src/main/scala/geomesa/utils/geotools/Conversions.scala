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

package geomesa.utils.geotools

import org.geotools.data.simple.SimpleFeatureIterator
import org.opengis.feature.simple.SimpleFeature
import org.geotools.util.{Converter, ConverterFactory}
import org.geotools.factory.Hints
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import org.opengis.temporal.{Position, Instant}
import org.geotools.temporal.`object`.{DefaultPosition, DefaultInstant, DefaultPeriod}

object Conversions {

  class RichSimpleFeatureIterator(iter: SimpleFeatureIterator) extends SimpleFeatureIterator
      with Iterator[SimpleFeature] {
    private[this] var open = true

    def isClosed = !open

    def hasNext = {
      if (isClosed) false
      if(iter.hasNext) true else{close(); false}
    }
    def next() = iter.next
    def close() { if(!isClosed) {iter.close(); open = false} }
  }

  implicit def toRichSimpleFeatureIterator(iter: SimpleFeatureIterator) = new RichSimpleFeatureIterator(iter)
  implicit def opengisInstantToJodaInstant(instant: Instant): org.joda.time.Instant = new DateTime(instant.getPosition.getDate).toInstant
  implicit def jodaInstantToOpengisInstant(instant: org.joda.time.Instant): org.opengis.temporal.Instant = new DefaultInstant(new DefaultPosition(instant.toDate))
  implicit def jodaIntervalToOpengisPeriod(interval: org.joda.time.Interval): org.opengis.temporal.Period =
    new DefaultPeriod(interval.getStart.toInstant, interval.getEnd.toInstant)

}

class JodaConverterFactory extends ConverterFactory {
  private val df = ISODateTimeFormat.dateTime()
  def createConverter(source: Class[_], target: Class[_], hints: Hints) =
    if(classOf[java.util.Date].isAssignableFrom(source) && classOf[String].isAssignableFrom(target)) {
      // Date => String
      new Converter {
        def convert[T](source: scala.Any, target: Class[T]): T =
          df.print(new DateTime(source.asInstanceOf[java.util.Date])).asInstanceOf[T]
      }
    } else if(classOf[java.util.Date].isAssignableFrom(target) && classOf[String].isAssignableFrom(source)) {
      // String => Date
      new Converter {
        def convert[T](source: scala.Any, target: Class[T]): T =
          df.parseDateTime(source.asInstanceOf[String]).toDate.asInstanceOf[T]
      }
    } else null.asInstanceOf[Converter]
}