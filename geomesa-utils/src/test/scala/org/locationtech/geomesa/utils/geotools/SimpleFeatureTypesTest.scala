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

    "handle namespaces" >> {
      "simple ones" >> {
        val sft = SimpleFeatureTypes.createType("ns:testing", "dtg:Date,*geom:Point:srid=4326")
        sft.getName.getLocalPart mustEqual "testing"
        sft.getName.getNamespaceURI mustEqual "ns"
        sft.getTypeName mustEqual("testing")
      }
      "complex ones" >> {
        val sft = SimpleFeatureTypes.createType("http://geomesa/ns:testing", "dtg:Date,*geom:Point:srid=4326")
        sft.getName.getLocalPart mustEqual "testing"
        sft.getName.getNamespaceURI mustEqual "http://geomesa/ns"
        sft.getTypeName mustEqual("testing")
      }
      "invalid ones" >> {
        val sft = SimpleFeatureTypes.createType("http://geomesa/ns:testing:", "dtg:Date,*geom:Point:srid=4326")
        sft.getName.getLocalPart mustEqual "http://geomesa/ns:testing:"
        sft.getName.getNamespaceURI must beNull
        sft.getTypeName mustEqual("http://geomesa/ns:testing:")
      }
    }

    "handle empty srid" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer:index=false,*geom:Point:index=true")
      (sft.getGeometryDescriptor.getCoordinateReferenceSystem must not).beNull
    }

    "handle Int vs. Integer lexicographical ordering" >> {
      val sft1 = SimpleFeatureTypes.createType("testing1", "foo:Int,*geom:Point:index=true")
      val sft2 = SimpleFeatureTypes.createType("testing2", "foo:Integer,*geom:Point:index=true")
      sft1.getAttributeCount must beEqualTo(2)
      sft2.getAttributeCount must beEqualTo(2)
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

    "return the indexed attributes (not including the default geometry)" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer:index=false,dtg:Date:index=true,*geom:Point:srid=4326:index=true")
      val indexed = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft)
      indexed.map(_.getLocalName) must containTheSameElementsAs(List("dtg"))
    }

    "set index=true for a default geometry" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer:index=false,dtg:Date:index=true,*geom:Point:srid=4326:index=false")
      sft.getGeometryDescriptor.getUserData.get("index").asInstanceOf[Boolean] must beTrue
    }

    "handle list types" >> {

      "with no values specified" >> {
        val sft = SimpleFeatureTypes.createType("testing", "id:Integer,names:List,dtg:Date,*geom:Point:srid=4326")
        sft.getAttributeCount mustEqual(4)
        sft.getDescriptor("names") must not beNull

        sft.getDescriptor("names").getType.getBinding mustEqual(classOf[java.util.List[_]])

        val spec = SimpleFeatureTypes.encodeType(sft)
        spec mustEqual "id:Integer:index=false,names:List[String]:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true"
      }

      "with defined values" >> {
        val sft = SimpleFeatureTypes.createType("testing", "id:Integer,names:List[Double],dtg:Date,*geom:Point:srid=4326")
        sft.getAttributeCount mustEqual(4)
        sft.getDescriptor("names") must not beNull

        sft.getDescriptor("names").getType.getBinding mustEqual(classOf[java.util.List[_]])

        val spec = SimpleFeatureTypes.encodeType(sft)
        spec mustEqual "id:Integer:index=false,names:List[Double]:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true"
      }

      "fail for illegal value format" >> {
        val spec = "id:Integer,names:List[Double][Double],dtg:Date,*geom:Point:srid=4326"
        SimpleFeatureTypes.createType("testing", spec) should throwAn[IllegalArgumentException]
      }

      "fail for illegal value classes" >> {
        val spec = "id:Integer,names:List[FAKE],dtg:Date,*geom:Point:srid=4326"
        SimpleFeatureTypes.createType("testing", spec) should throwAn[IllegalArgumentException]
      }
    }

    "handle map types" >> {

      "with no values specified" >> {
        val sft = SimpleFeatureTypes.createType("testing", "id:Integer,metadata:Map,dtg:Date,*geom:Point:srid=4326")
        sft.getAttributeCount mustEqual(4)
        sft.getDescriptor("metadata") must not beNull

        sft.getDescriptor("metadata").getType.getBinding mustEqual classOf[java.util.Map[_, _]]

        val spec = SimpleFeatureTypes.encodeType(sft)
        spec mustEqual "id:Integer:index=false,metadata:Map[String,String]:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true"
      }

      "with defined values" >> {
        val sft = SimpleFeatureTypes.createType("testing", "id:Integer,metadata:Map[Double,String],dtg:Date,*geom:Point:srid=4326")
        sft.getAttributeCount mustEqual(4)
        sft.getDescriptor("metadata") must not beNull

        sft.getDescriptor("metadata").getType.getBinding mustEqual classOf[java.util.Map[_, _]]

        val spec = SimpleFeatureTypes.encodeType(sft)
        spec mustEqual "id:Integer:index=false,metadata:Map[Double,String]:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true"
      }

      "fail for illegal value format" >> {
        val spec = "id:Integer,metadata:Map[String],dtg:Date,*geom:Point:srid=4326"
        SimpleFeatureTypes.createType("testing", spec) should throwAn[IllegalArgumentException]
      }

      "fail for illegal value classes" >> {
        val spec = "id:Integer,metadata:Map[String,FAKE],dtg:Date,*geom:Point:srid=4326"
        SimpleFeatureTypes.createType("testing", spec) should throwAn[IllegalArgumentException]
      }
    }

    "handle splitter and splitter options" >> {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;table.splitter.class=org.locationtech.geomesa.core.data.DigitSplitter,table.splitter.options=fmt:%02d,min:0,max:99"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getUserData.get(SimpleFeatureTypes.TABLE_SPLITTER) must be equalTo "org.locationtech.geomesa.core.data.DigitSplitter"
      val opts = sft.getUserData.get(SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS).asInstanceOf[Map[String, String]]
      opts.size must be equalTo 3
      opts("fmt") must be equalTo "%02d"
      opts("min") must be equalTo "0"
      opts("max") must be equalTo "99"
    }
  }

}
