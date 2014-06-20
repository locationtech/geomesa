package geomesa.plugin.process

import java.io.{InputStream, OutputStream, Writer}
import javax.xml.namespace.QName

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.json.{AbstractJsonWriter, JsonHierarchicalStreamDriver, JsonWriter}
import geomesa.core.process.rank.ResultBean
import org.geoserver.wps.ppio.{CDataPPIO, XStreamPPIO}

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/19/14
 * Time: 6:02 PM
 */
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
