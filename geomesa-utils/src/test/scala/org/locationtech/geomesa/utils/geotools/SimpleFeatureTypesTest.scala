/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.regex.Pattern

import com.typesafe.config.ConfigFactory
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs._
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecConfig
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.locationtech.geomesa.utils.text.KVPairParser
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class SimpleFeatureTypesTest extends Specification {

  sequential
  args(color = true)

  "SimpleFeatureTypes" should {
    "create an sft that" >> {
      val sft = SimpleFeatureTypes.createType("testing", "id:Integer,dtg:Date,*geom:Point:srid=4326:index=true")
      "has name \'test\'"  >> { sft.getTypeName mustEqual "testing" }
      "has three attributes" >> { sft.getAttributeCount must be_==(3) }
      "has an id attribute which is " >> {
        val idDescriptor = sft.getDescriptor("id")
        "not null"    >> { (idDescriptor must not).beNull }
        "not indexed" >> { idDescriptor.getUserData.get("index") must beNull }
      }
      "has a default geom field called 'geom'" >> {
        val geomDescriptor = sft.getGeometryDescriptor
        geomDescriptor.getLocalName must be equalTo "geom"
      }
      "not include index flag for geometry" >> {
        val geomDescriptor = sft.getGeometryDescriptor
        geomDescriptor.getUserData.get("index") must beNull
      }
      "encode an sft properly" >> {
        SimpleFeatureTypes.encodeType(sft) must be equalTo s"id:Integer,dtg:Date,*geom:Point:srid=4326"
      }
      "encode an sft properly without user data" >> {
        sft.getUserData.put("geomesa.table.sharing", "true")
        sft.getUserData.put("hello", "goodbye")
        SimpleFeatureTypes.encodeType(sft) must be equalTo s"id:Integer,dtg:Date,*geom:Point:srid=4326"
      }
      "encode an sft properly with geomesa user data" >> {
        val encoded = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
        encoded must startWith("id:Integer,dtg:Date,*geom:Point:srid=4326;")
        encoded must contain("geomesa.index.dtg='dtg'")
        encoded must contain("geomesa.table.sharing='true'")
        encoded must not(contain("hello="))
      }
      "encode an sft properly with specified user data" >> {
        import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
        sft.setUserDataPrefixes(Seq("hello"))
        val encoded = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
        encoded must startWith("id:Integer,dtg:Date,*geom:Point:srid=4326;")
        encoded must contain("geomesa.user-data.prefix='hello'")
        encoded must contain("geomesa.index.dtg='dtg'")
        encoded must contain("geomesa.table.sharing='true'")
        encoded must contain("hello='goodbye'")
      }
    }

    "create an empty type" >> {
      val sft = SimpleFeatureTypes.createType("test", "")
      sft.getTypeName mustEqual "test"
      sft.getAttributeDescriptors must beEmpty
    }

    "create an empty type with user data" >> {
      val sft = SimpleFeatureTypes.createType("test", ";geomesa.table.sharing='true'")
      sft.getTypeName mustEqual "test"
      sft.getAttributeDescriptors must beEmpty
      sft.getUserData.get("geomesa.table.sharing") mustEqual "true"
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
      sft.getDescriptor("id").getUserData.get(AttributeOptions.OPT_INDEX) must beNull
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
      val indexed = sft.getAttributeDescriptors.collect {
        case d if java.lang.Boolean.valueOf(d.getUserData.get(AttributeOptions.OPT_INDEX).asInstanceOf[String]) => d.getLocalName
      }
      indexed mustEqual List("dtg")
    }

    "handle list types" >> {

      "with no values specified" >> {
        val sft = SimpleFeatureTypes.createType("testing", "id:Integer,names:List,dtg:Date,*geom:Point:srid=4326")
        sft.getAttributeCount mustEqual(4)
        sft.getDescriptor("names") must not beNull

        sft.getDescriptor("names").getType.getBinding mustEqual(classOf[java.util.List[_]])

        val spec = SimpleFeatureTypes.encodeType(sft)
        spec mustEqual s"id:Integer,names:List[String],dtg:Date,*geom:Point:srid=4326"
      }

      "with defined values" >> {
        val sft = SimpleFeatureTypes.createType("testing", "id:Integer,names:List[Double],dtg:Date,*geom:Point:srid=4326")
        sft.getAttributeCount mustEqual(4)
        sft.getDescriptor("names") must not beNull

        sft.getDescriptor("names").getType.getBinding mustEqual(classOf[java.util.List[_]])

        val spec = SimpleFeatureTypes.encodeType(sft)
        spec mustEqual s"id:Integer,names:List[Double],dtg:Date,*geom:Point:srid=4326"
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
        spec mustEqual s"id:Integer,metadata:Map[String,String],dtg:Date,*geom:Point:srid=4326"
      }

      "with defined values" >> {
        val sft = SimpleFeatureTypes.createType("testing", "id:Integer,metadata:Map[Double,String],dtg:Date,*geom:Point:srid=4326")
        sft.getAttributeCount mustEqual(4)
        sft.getDescriptor("metadata") must not beNull

        sft.getDescriptor("metadata").getType.getBinding mustEqual classOf[java.util.Map[_, _]]

        val spec = SimpleFeatureTypes.encodeType(sft)
        spec mustEqual s"id:Integer,metadata:Map[Double,String],dtg:Date,*geom:Point:srid=4326"
      }

      "with a byte array as a value" >> {
        val sft = SimpleFeatureTypes.createType("testing", "byteMap:Map[String,Bytes]")
        sft.getAttributeCount mustEqual(1)
        sft.getDescriptor("byteMap") must not beNull

        sft.getDescriptor("byteMap").getType.getBinding mustEqual classOf[java.util.Map[_, _]]

        val spec = SimpleFeatureTypes.encodeType(sft)
        spec mustEqual s"byteMap:Map[String,Bytes]"
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
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.{TABLE_SPLITTER, TABLE_SPLITTER_OPTS}

      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;table.splitter.class=org.locationtech.geomesa.core.data.DigitSplitter,table.splitter.options='fmt:%02d,min:0,max:99'"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getUserData.get(TABLE_SPLITTER) must be equalTo "org.locationtech.geomesa.core.data.DigitSplitter"
      val opts = KVPairParser.parse(sft.getUserData.get(TABLE_SPLITTER_OPTS).asInstanceOf[String])
      opts must haveSize(3)
      opts.get("fmt") must beSome("%02d")
      opts.get("min") must beSome("0")
      opts.get("max") must beSome("99")
    }

    "handle enabled indexes" >> {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='z2,id,z3'"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getUserData.get(ENABLED_INDICES).toString.split(",").toList must be equalTo List("z2", "id", "z3")
    }

    "handle splitter opts and enabled indexes" >> {
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.{TABLE_SPLITTER, TABLE_SPLITTER_OPTS}

      val specs = List(
        "name:String,dtg:Date,*geom:Point:srid=4326;table.splitter.class=org.locationtech.geomesa.core.data.DigitSplitter,table.splitter.options='fmt:%02d,min:0,max:99',geomesa.indices.enabled='z2,id,z3'",
        "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='z2,id,z3',table.splitter.class=org.locationtech.geomesa.core.data.DigitSplitter,table.splitter.options='fmt:%02d,min:0,max:99'")
      specs.forall { spec =>
        val sft = SimpleFeatureTypes.createType("test", spec)
        sft.getUserData.get(TABLE_SPLITTER) must be equalTo "org.locationtech.geomesa.core.data.DigitSplitter"
        val opts = KVPairParser.parse(sft.getUserData.get(TABLE_SPLITTER_OPTS).asInstanceOf[String])
        opts must haveSize(3)
        opts.get("fmt") must beSome("%02d")
        opts.get("min") must beSome("0")
        opts.get("max") must beSome("99")
        sft.getUserData.get(ENABLED_INDICES).toString.split(",").toList must be equalTo List("z2", "id", "z3")
      }
    }

    "allow arbitrary feature options in user data" >> {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;a='',c=d,x=',,,',z=23562356"
      val sft = SimpleFeatureTypes.createType("foobar", spec)
      sft.getUserData.toList must containAllOf(Seq("a" -> "", "c" -> "d", "x" -> ",,,", "z" -> "23562356"))
    }

    "allow user data with a unicode character" >> {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.table.sharing.prefix='\\u0001',geomesa.mixed.geometries='true',table.indexes.enabled='',geomesa.table.sharing='true',geomesa.all.user.data='true'"
      val sft = SimpleFeatureTypes.createType("foobar", spec)
      sft.getUserData.toList must containAllOf(Seq("geomesa.table.sharing.prefix" -> "\u0001", "geomesa.mixed.geometries" -> "true", "geomesa.table.sharing" -> "true"))
    }

    "allow specification of ST index entry values" >> {
      val spec = s"name:String:index=true:$OPT_INDEX_VALUE=true,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getDescriptor("name").isIndexValue() must beTrue
    }

    "allow specification of attribute cardinality" >> {
      val spec = s"name:String:$OPT_CARDINALITY=high,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getDescriptor("name").getUserData.get(OPT_CARDINALITY) mustEqual("high")
      sft.getDescriptor("name").getCardinality() mustEqual(Cardinality.HIGH)
    }

    "allow specification of attribute cardinality regardless of case" >> {
      val spec = s"name:String:$OPT_CARDINALITY=LOW,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getDescriptor("name").getUserData.get(OPT_CARDINALITY) mustEqual("low")
      sft.getDescriptor("name").getCardinality() mustEqual(Cardinality.LOW)
    }.pendingUntilFixed("currently case sensitive")

    "allow specification of index attribute coverages" >> {
      val spec = s"name:String:$OPT_INDEX=join,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getDescriptor("name").getUserData.get(OPT_INDEX) mustEqual("join")
    }

    "allow specification of index attribute coverages regardless of case" >> {
      val spec = s"name:String:$OPT_INDEX=FULL,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getDescriptor("name").getUserData.get(OPT_INDEX) mustEqual("full")
    }.pendingUntilFixed("currently case sensitive")

    "allow specification of index attribute coverages as booleans" >> {
      val spec = s"name:String:$OPT_INDEX=true,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getDescriptor("name").getUserData.get(OPT_INDEX) mustEqual("true")
    }

    "allow attribute options with quoted commas" >> {
      val spec = s"name:String:foo='bar,baz',dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getDescriptor("name").getUserData.get("foo") mustEqual "bar,baz"
    }

    "round trip attribute options with quoted commas" >> {
      val spec = s"name:String:foo='bar,baz',dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", spec)
      sft.getDescriptor("name").getUserData.get("foo") mustEqual "bar,baz"
      val encoded = SimpleFeatureTypes.encodeType(sft)
      val recovered = SimpleFeatureTypes.createType("test", encoded)
      recovered.getDescriptor("name").getUserData.get("foo") mustEqual "bar,baz"
    }

    "encode date attribute types" >> {
      val sft: SimpleFeatureType = {
        val builder = new SimpleFeatureTypeBuilder()
        builder.setName("test")
        builder.add("date", classOf[java.util.Date])
        builder.add("sqlDate", classOf[java.sql.Date])
        builder.add("sqlTimestamp", classOf[java.sql.Timestamp])
        builder.buildFeatureType()
      }
      SimpleFeatureTypes.encodeDescriptor(sft, sft.getDescriptor(0)) mustEqual "date:Date"
      SimpleFeatureTypes.encodeDescriptor(sft, sft.getDescriptor(1)) mustEqual "sqlDate:Date"
      SimpleFeatureTypes.encodeDescriptor(sft, sft.getDescriptor(2)) mustEqual "sqlTimestamp:Timestamp"
    }

    "create schemas from sql dates" >> {
      SimpleFeatureTypes.createType("test", "dtg:Timestamp,*geom:Point:srid=4326")
          .getDescriptor(0).getType.getBinding mustEqual classOf[java.sql.Timestamp]
    }

    "return meaningful error messages" >> {
      Try(SimpleFeatureTypes.createType("test", null)) must
          beAFailedTry.withThrowable[IllegalArgumentException](Pattern.quote("Invalid spec string: null"))
      val failures = Seq(
        ("foo:Strong", "7. Expected attribute type binding"),
        ("foo:String,*bar:String", "16. Expected geometry type binding"),
        ("foo:String,bar:String;;", "22. Expected one of: feature type option, end of spec"),
        ("foo:String,bar,baz:String", "14. Expected one of: attribute name, attribute type binding, geometry type binding"),
        ("foo:String:bar,baz:String", "14. Expected attribute option")
      )
      forall(failures) { case (spec, message) =>
        val pattern = Pattern.quote(s"Invalid spec string at index $message.")
        val result = Try(SimpleFeatureTypes.createType("test", spec))
        result must beAFailedTry.withThrowable[IllegalArgumentException](pattern)
      }
    }

    "build from conf" >> {

      def doTest(sft: SimpleFeatureType) = {
        sft.getAttributeCount must be equalTo 4
        sft.getGeometryDescriptor.getName.getLocalPart must be equalTo "geom"
        sft.getDescriptor("testStr").getCardinality() mustEqual(Cardinality.UNKNOWN)
        sft.getDescriptor("testCard").getCardinality() mustEqual(Cardinality.HIGH)
        sft.getTypeName must be equalTo "testconf"
      }

      "with no path" >> {
        val regular = ConfigFactory.parseString(
          """
            |{
            |  type-name = "testconf"
            |  fields = [
            |    { name = "testStr",  type = "string"       , index = true  },
            |    { name = "testCard", type = "string"       , index = true, cardinality = high },
            |    { name = "testList", type = "List[String]" , index = false },
            |    { name = "geom",     type = "Point"        , srid = 4326, default = true }
            |  ]
            |}
          """.stripMargin)
        val sftRegular = SimpleFeatureTypes.createType(regular)
        doTest(sftRegular)
      }

      "with some nesting path" >>{
        val someNesting = ConfigFactory.parseString(
          """
            |{
            |  foobar = {
            |    type-name = "testconf"
            |    fields = [
            |      { name = "testStr",  type = "string"       , index = true  },
            |      { name = "testCard", type = "string"       , index = true, cardinality = high },
            |      { name = "testList", type = "List[String]" , index = false },
            |      { name = "geom",     type = "Point"        , srid = 4326, default = true }
            |    ]
            |  }
            |}
          """.stripMargin)
        val someSft = SimpleFeatureTypes.createType(someNesting, path = Some("foobar"))
        doTest(someSft)
      }

      "with multiple nested paths" >> {
        val customNesting = ConfigFactory.parseString(
          """
            |baz = {
            |  foobar = {
            |    type-name = "testconf"
            |    fields = [
            |      { name = "testStr",  type = "string"       , index = true  },
            |      { name = "testCard", type = "string"       , index = true, cardinality = high },
            |      { name = "testList", type = "List[String]" , index = false },
            |      { name = "geom",     type = "Point"        , srid = 4326, default = true }
            |    ]
            |  }
            |}
          """.stripMargin)

        val sftCustom = SimpleFeatureTypes.createType(customNesting, path = Some("baz.foobar"))
        doTest(sftCustom)
      }
    }

    "build from default nested conf" >> {
      val conf = ConfigFactory.parseString(
        """
          |sft = {
          |  type-name = "testconf"
          |  fields = [
          |    { name = "testStr",  type = "string"       , index = true  },
          |    { name = "testCard", type = "string"       , index = true, cardinality = high },
          |    { name = "testList", type = "List[String]" , index = false },
          |    { name = "geom",     type = "Point"        , srid = 4326, default = true }
          |  ]
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(conf)
      sft.getAttributeCount must be equalTo 4
      sft.getGeometryDescriptor.getName.getLocalPart must be equalTo "geom"
      sft.getDescriptor("testStr").getCardinality() mustEqual(Cardinality.UNKNOWN)
      sft.getDescriptor("testCard").getCardinality() mustEqual(Cardinality.HIGH)
      sft.getTypeName must be equalTo "testconf"
    }


    "allow user data in conf" >> {
      val conf = ConfigFactory.parseString(
        """
          |{
          |  type-name = "testconf"
          |  fields = [
          |    { name = "testStr",  type = "string"       , index = true  },
          |    { name = "testCard", type = "string"       , index = true, cardinality = high },
          |    { name = "testList", type = "List[String]" , index = false },
          |    { name = "geom",     type = "Point"        , srid = 4326, default = true }
          |  ]
          |  user-data = {
          |    mydataone = true
          |    mydatatwo = "two"
          |  }
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(conf)
      sft.getAttributeCount must be equalTo 4
      sft.getGeometryDescriptor.getName.getLocalPart must be equalTo "geom"
      sft.getDescriptor("testStr").getCardinality() mustEqual(Cardinality.UNKNOWN)
      sft.getDescriptor("testCard").getCardinality() mustEqual(Cardinality.HIGH)
      sft.getTypeName must be equalTo "testconf"
      sft.getUserData.size() mustEqual 2
      sft.getUserData.get("mydataone") mustEqual "true"
      sft.getUserData.get("mydatatwo") mustEqual "two"
    }

    "allow quoted and unquoted keys in user data conf" >> {
      val conf = ConfigFactory.parseString(
        """
          |{
          |  type-name = "testconf"
          |  fields = [
          |    { name = "testStr",  type = "string"       , index = true  },
          |    { name = "testCard", type = "string"       , index = true, cardinality = high },
          |    { name = "testList", type = "List[String]" , index = false },
          |    { name = "geom",     type = "Point"        , srid = 4326, default = true }
          |  ]
          |  user-data = {
          |    "mydataone" = true
          |    mydatatwo = "two"
          |    "my.data.three" = "three"
          |    my.data.four = "four"
          |  }
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(conf)
      sft.getUserData.size() mustEqual 4
      sft.getUserData.get("mydataone") mustEqual "true"
      sft.getUserData.get("mydatatwo") mustEqual "two"
      sft.getUserData.get("my.data.three") mustEqual "three"
      sft.getUserData.get("my.data.four") mustEqual "four"
    }

    "untyped lists and maps as a type" >> {
      val conf = ConfigFactory.parseString(
        """
          |{
          |  type-name = "testconf"
          |  fields = [
          |    { name = "testList", type = "List"  , index = false },
          |    { name = "testMap",  type = "Map"   , index = false },
          |    { name = "geom",     type = "Point" , srid = 4326, default = true }
          |  ]
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(conf)
      sft.getAttributeCount must be equalTo 3
      sft.getGeometryDescriptor.getName.getLocalPart must be equalTo "geom"
      sft.getAttributeDescriptors.get(0).getType.getBinding must beAssignableFrom[java.util.List[_]]
      sft.getAttributeDescriptors.get(1).getType.getBinding must beAssignableFrom[java.util.Map[_,_]]
    }

    "bytes as a type to work" >> {
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs._
      val conf = ConfigFactory.parseString(
        """
          |{
          |  type-name = "byteconf"
          |  fields = [
          |    { name = "blob",     type = "Bytes",              index = false }
          |    { name = "blobList", type = "List[Bytes]",        index = false }
          |    { name = "blobMap",  type = "Map[String, Bytes]", index = false }
          |  ]
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(conf)
      sft.getAttributeCount must be equalTo 3
      sft.getAttributeDescriptors.get(0).getType.getBinding must beAssignableFrom[Array[Byte]]
      sft.getAttributeDescriptors.get(1).getType.getBinding must beAssignableFrom[java.util.List[_]]
      sft.getAttributeDescriptors.get(1).getUserData.get(USER_DATA_LIST_TYPE) mustEqual classOf[Array[Byte]].getName
      sft.getAttributeDescriptors.get(2).getType.getBinding must beAssignableFrom[java.util.Map[_,_]]
      sft.getAttributeDescriptors.get(2).getUserData.get(USER_DATA_MAP_KEY_TYPE) mustEqual classOf[String].getName
      sft.getAttributeDescriptors.get(2).getUserData.get(USER_DATA_MAP_VALUE_TYPE) mustEqual classOf[Array[Byte]].getName
    }

    "render SFTs as config again" >> {
      import scala.collection.JavaConverters._
      val conf = ConfigFactory.parseString(
        """
          |{
          |  type-name = "testconf"
          |  fields = [
          |    { name = "testStr",  type = "string"             , index = true   }
          |    { name = "testCard", type = "string"             , index = true,  cardinality = high }
          |    { name = "testList", type = "List[String]"       , index = false  }
          |    { name = "testMap",  type = "Map[String, String]", index = false  }
          |    { name = "dtg",      type = "Date"                                }
          |    { name = "dtg2",     type = "Date"                                } // not default because of ordering
          |    { name = "geom",     type = "Point" , srid = 4326, default = true }
          |  ]
          |  user-data = {
          |    "geomesa.one" = "true"
          |    geomesa.two = "two"
          |  }
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(conf)
      val typeConf = SimpleFeatureTypes.toConfig(sft).getConfig("geomesa.sfts.testconf")
      typeConf.getString("type-name") mustEqual "testconf"

      def getFieldOpts(s: String) =
        typeConf.getConfigList("attributes").filter(_.getString("name") == s).get(0).entrySet().map { case e =>
          e.getKey -> e.getValue.unwrapped()
        }.toMap

      getFieldOpts("testStr") must havePairs("name" -> "testStr", "type" -> "String", "index" -> "true")
      getFieldOpts("testCard") must havePairs("name" -> "testCard", "type" -> "String",
        "index" -> "true", "cardinality" -> "high")
      getFieldOpts("testList") must havePairs("name" -> "testList", "type" -> "List[String]")
      getFieldOpts("testMap") must havePairs("name" -> "testMap", "type" -> "Map[String,String]")
      getFieldOpts("dtg") must havePairs("name" -> "dtg", "type" -> "Date", "default" -> "true")
      getFieldOpts("dtg2") must havePairs("name" -> "dtg2", "type" -> "Date")
      getFieldOpts("geom") must havePairs("name" -> "geom", "type" -> "Point",
        "srid" -> "4326", "default" -> "true")

      val userdata = typeConf.getConfig(SimpleFeatureSpecConfig.UserDataPath).root.unwrapped().asScala
      userdata must havePairs("geomesa.one" -> "true", "geomesa.two" -> "two")
    }

    "render sfts as config with table sharing" >> {
      import scala.collection.JavaConverters._
      val sft = SimpleFeatureTypes.createType("geolife",
        "userId:String,trackId:String,altitude:Double,dtg:Date,*geom:Point:srid=4326;" +
            "geomesa.index.dtg='dtg',geomesa.table.sharing='true',geomesa.indices='z3:4:3,z2:3:3,id:2:3'," +
            "geomesa.table.sharing.prefix='\\u0001'")
      val config = SimpleFeatureTypes.toConfig(sft, includePrefix = false)
      config.hasPath(SimpleFeatureSpecConfig.UserDataPath) must beTrue
      val userData = config.getConfig(SimpleFeatureSpecConfig.UserDataPath)
      userData.root.unwrapped().asScala mustEqual Map(
        "geomesa.index.dtg" -> "dtg",
        "geomesa.indices" -> "z3:4:3,z2:3:3,id:2:3",
        "geomesa.table.sharing" -> "true",
        "geomesa.table.sharing.prefix" -> "\u0001"
      )
    }
  }

}
