package geomesa.core.process

import collection.JavaConversions._
import com.vividsolutions.jts.geom.GeometryCollection
import geomesa.process.TubeVisitor
import geomesa.utils.text.WKTUtils
import org.geotools.data.DataUtilities
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.apache.log4j.Logger


@RunWith(classOf[JUnitRunner])
class TubeBinTest extends Specification {

  sequential

  private val log = Logger.getLogger(classOf[TubeBinTest])

  val geotimeAttributes = geomesa.core.index.spec

  "TubeVisitor" should {

    "correctly time bin features" in {
      val sftName = "tubetest2"
      val sft = DataUtilities.createType(sftName, s"type:String,$geotimeAttributes")

      val list = for(day <- 1 until 20) yield {
        val sf = SimpleFeatureBuilder.build(sft, List(), day.toString)
        val lat = 40+day
        sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
        sf.setAttribute(geomesa.core.index.SF_PROPERTY_START_TIME, new DateTime(f"2011-01-$day%02dT00:00:00Z", DateTimeZone.UTC).toDate)
        sf.setAttribute("type","test")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        sf
      }

      log.debug("features: "+list.size)
      val binnedFeatures = TubeVisitor.timeBinAndUnion(list, 6)

      binnedFeatures.foreach { sf =>
        if (sf.getDefaultGeometry.isInstanceOf[GeometryCollection])
          log.debug("size: " + sf.getDefaultGeometry.asInstanceOf[GeometryCollection].getNumGeometries +" "+ sf.getDefaultGeometry)
        else log.debug("size: 1")
      }

      TubeVisitor.timeBinAndUnion(list, 1).size should equalTo(1)

      TubeVisitor.timeBinAndUnion(list, 0).size should equalTo(1)
    }

  }
}
