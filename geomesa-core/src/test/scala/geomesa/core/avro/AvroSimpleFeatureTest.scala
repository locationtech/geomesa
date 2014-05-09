package geomesa.core.avro

import collection.JavaConversions._
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureTest extends Specification {

  "AvroSimpleFeature" should {
    "properly convert attributes that are set as strings" in {
      val sft = DataUtilities.createType("testType", "a:Integer,b:Date,*geom:Point:srid=4326")

      val f = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)
      f.setAttribute(0,"1")
      f.setAttribute(1,"2013-01-02T00:00:00.000Z")
      f.setAttribute(2,"POINT(45.0 49.0)")

      f.getAttribute(0) must beAnInstanceOf[java.lang.Integer]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }
  }

  "AvroSimpleFeature" should {
    "properly validate a correct object" in {
      val sft = DataUtilities.createType("testType", "a:Integer,b:Date,*geom:Point:srid=4326")

      val f = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)
      f.setAttribute(0,"1")
      f.setAttribute(1,"2013-01-02T00:00:00.000Z") // this date format should be converted
      f.setAttribute(2,"POINT(45.0 49.0)")

      f.validate must not(throwA [org.opengis.feature.IllegalAttributeException])
    }

    "fail to validate a correct object" in {
      val sft = DataUtilities.createType("testType", "a:Integer,b:Date,*geom:Point:srid=4326")

      val f = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)
      f.setAttribute(0,"1")
      f.setAttributeNoConvert(1, "2013-01-02T00:00:00.000Z") // don't convert
      f.setAttribute(2,"POINT(45.0 49.0)")

      f.validate must throwA [org.opengis.feature.IllegalAttributeException]  //should throw it
    }

    "properly convert empty strings to null" in {
      val sft = DataUtilities.createType(
        "testType",
        "a:Integer,b:Float,c:Double,d:Boolean,e:Date,f:UUID,g:Point"+
        ",h:LineString,i:Polygon,j:MultiPoint,k:MultiLineString"+
        ",l:MultiPolygon,m:GeometryCollection"
      )


      val f = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)
      f.setAttribute("a","")
      f.setAttribute("b","")
      f.setAttribute("c","")
      f.setAttribute("d","")
      f.setAttribute("e","")
      f.setAttribute("f","")
      f.setAttribute("g","")
      f.setAttribute("h","")
      f.setAttribute("i","")
      f.setAttribute("j","")
      f.setAttribute("k","")
      f.setAttribute("l","")
      f.setAttribute("m","")

      f.getAttributes.foreach { v => v must beNull}

      f.validate must not(throwA [org.opengis.feature.IllegalAttributeException])
    }
  }

  "AvroSimpleFeature" should {
    "give back a null when an attribute doesn't exist" in {

      // Verify that AvroSimpleFeature returns null for attributes that do not exist like SimpleFeatureImpl
      val sft = DataUtilities.createType("avrotesttype", "a:Integer,b:String")
      val sf = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)
      sf.getAttribute("c") must not(throwA[NullPointerException])
      sf.getAttribute("c") should beNull

      val oldSf = new SimpleFeatureImpl(List(null, null), sft, new FeatureIdImpl("fakeid"))
      oldSf.getAttribute("c") should beNull
    }
  }
}
