package geomesa.utils.geotools

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureTypesTest extends Specification {

  args(color = true)

  "SimpleFeatureTypes" should {
    "create an sft that" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true")
      "has name \'test\'"  >> { sft.getTypeName mustEqual "testing" }
      "has three attributes" >> { sft.getAttributeCount must be_==(3) }
      "has an id attribute which is " >> {
        val idDescriptor = sft.getDescriptor("id")
        "not null"    >> { (idDescriptor must not).beNull }
        "not indexed" >> { idDescriptor.getUserData.get("index").asInstanceOf[Boolean] must beFalse }
      }
      "has a default geom field called 'geom'" >> {
        val geomDescriptor = sft.getGeometryDescriptor
        geomDescriptor.getLocalName must be equalTo "geom"
      }

      "encode an sft properly" >> {
        SimpleFeatureTypes.encodeType(sft) must be equalTo "id:Integer:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true"
      }
    }

    "handle empty srid" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer:index=false,*geom:Point:index=true")
      (sft.getGeometryDescriptor.getCoordinateReferenceSystem must not).beNull
    }

    "handle no index attribute" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer,*geom:Point:index=true")
      sft.getDescriptor("id").getUserData.containsKey("index") must beTrue
      sft.getDescriptor("id").getUserData.get("index").asInstanceOf[Boolean] must beFalse
    }

    "handle no explicit geometry" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer,geom:Point:index=true,geom2:Geometry")
      sft.getGeometryDescriptor.getLocalName must be equalTo "geom"
    }

    "handle a namespace" >> {
      val sft = SimpleFeatureTypes.createType("foo:testing", "id:Integer,geom:Point:index=true,geom2:Geometry")
      sft.getName.getNamespaceURI must be equalTo "foo"
    }

    "return the indexed attributes" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer:index=false,dtg:Date:index=true,*geom:Point:srid=4326:index=true")
      val indexed = SimpleFeatureTypes.getIndexedAttributes(sft)
      indexed.map(_.getLocalName) must containTheSameElementsAs(List("dtg", "geom"))
    }
  }


}
