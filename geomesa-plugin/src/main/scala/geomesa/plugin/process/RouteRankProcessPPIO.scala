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

package geomesa.plugin.process

import java.io.{InputStream, OutputStream, Writer}
import javax.xml.namespace.QName

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.json.{AbstractJsonWriter, JsonHierarchicalStreamDriver, JsonWriter}
import geomesa.core.process.rank.ResultBean
import org.geoserver.wps.ppio.{CDataPPIO, XStreamPPIO}

class RouteRankProcessPPIO extends XStreamPPIO(classOf[ResultBean],
  new QName("RankingResults"))

class XStreamJsonPPIO[T](objType: Class[T]) extends CDataPPIO(objType, objType, "application/json") {
  def buildXStream() = {
    new XStream(new JsonHierarchicalStreamDriver() {
      override def createWriter(out: Writer) = {
        new JsonWriter(out, AbstractJsonWriter.DROP_ROOT_MODE)
      }
    })
  }

  override def decode(p1: String) = throw new UnsupportedOperationException("JSON parsing is not supported")

  override def decode(is: InputStream) = throw new UnsupportedOperationException("JSON parsing is not supported")

  override def encode(value: scala.Any, os: OutputStream) {
    val xstream = buildXStream()
    xstream.toXML(value, os)
  }

  override def getFileExtension = "json"
}

class RouteRankProcessJsonPPIO extends XStreamJsonPPIO(classOf[ResultBean])
