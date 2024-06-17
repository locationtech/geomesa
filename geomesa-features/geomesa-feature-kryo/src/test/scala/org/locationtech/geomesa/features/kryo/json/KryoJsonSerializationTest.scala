/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import com.esotericsoftware.kryo.io.{Input, Output}
import org.junit.runner.RunWith
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KryoJsonSerializationTest extends Specification {

  import scala.collection.JavaConverters._

  val geoms = Seq(
    """{ "type": "Point", "coordinates": [30, 10] }""",
    """{ "type": "LineString", "coordinates": [[30, 10], [10, 30], [40, 40]] }""",
    """{ "type": "Polygon", "coordinates": [[[30, 10], [40, 40], [20, 40], [10, 20], [30, 10]]] }""",
    """{ "type": "Polygon", "coordinates": [
      |    [[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]],
      |    [[20, 30], [35, 35], [30, 20], [20, 30]]
      |  ]
      |}""".stripMargin,
    """{ "type": "MultiPoint", "coordinates": [[10, 40], [40, 30], [20, 20], [30, 10]] }""",
    """{ "type": "MultiLineString", "coordinates": [
      |    [[10, 10], [20, 20], [10, 40]],
      |    [[40, 40], [30, 30], [40, 20], [30, 10]]
      |  ]
      |}
    """.stripMargin,
    """{ "type": "MultiPolygon", "coordinates": [
      |        [[[30, 20], [45, 40], [10, 40], [30, 20]]],
      |        [[[15, 5], [40, 10], [10, 20], [5, 10], [15, 5]]]
      |      ]
      |    }
    """.stripMargin,
    """{ "type": "MultiPolygon", "coordinates": [
      |    [[[40, 40], [20, 45], [45, 30], [40, 40]]],
      |    [[[20, 35], [10, 30], [10, 10], [30, 5], [45, 20], [20, 35]],[[30, 20], [20, 15], [20, 25], [30, 20]]]
      |  ]
      |}
    """.stripMargin
  )

  val bookJson =
    """{
      |  "store": {
      |    "book": [
      |      {
      |        "category": "reference",
      |        "author": "Nigel Rees",
      |        "title": "Sayings of the Century",
      |        "price": 8.95
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "Evelyn Waugh",
      |        "title": "Sword of Honour",
      |        "price": 12.99
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "Herman Melville",
      |        "title": "Moby Dick",
      |        "isbn": "0-553-21311-3",
      |        "price": 8.99
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "J. R. R. Tolkien",
      |        "title": "The Lord of the Rings",
      |        "isbn": "0-395-19395-8",
      |        "price": 22.99
      |      }
      |    ],
      |    "bicycle": {
      |      "color": "red",
      |      "price": 19.95
      |    }
      |  },
      |  "expensive": 10
      |}""".stripMargin

  val books = Seq(
    """{"category":"reference","author":"Nigel Rees","title":"Sayings of the Century","price":8.95}""",
    """{"category":"fiction","author":"Evelyn Waugh","title":"Sword of Honour","price":12.99}""",
    """{"category":"fiction","author":"Herman Melville","title":"Moby Dick","isbn":"0-553-21311-3","price":8.99}""",
    """{"category":"fiction","author":"J. R. R. Tolkien","title":"The Lord of the Rings","isbn":"0-395-19395-8","price":22.99}""",
  )

  "KryoJsonSerialization" should {
    "correctly de/serialize null" in {
      val out = new Output(128)
      KryoJsonSerialization.serialize(out, null.asInstanceOf[String])
      val bytes = out.toBytes
      bytes must not(beEmpty)
      KryoJsonSerialization.deserializeAndRender(new Input(bytes)) must beNull
      KryoJsonSerialization.deserialize(new Input(bytes)) must beNull
    }
    "correctly de/serialize basic json" in {
      val out = new Output(128)
      val json = """{ "foo" : false, "bar" : "yes", "baz" : [ 1, 2, 3 ] }"""
      KryoJsonSerialization.serialize(out, json)
      val bytes = out.toBytes
      bytes must not(beEmpty)
      val recovered = KryoJsonSerialization.deserializeAndRender(new Input(bytes))
      recovered mustEqual json.replaceAll(" ", "")
    }
    "correctly de/serialize large json requiring buffer resizing" in {
      val out = new Output(128, -1)
      val string = "a" * 128
      val json = s"""{ "foo" : "$string" }"""
      KryoJsonSerialization.serialize(out, json)
      val bytes = out.toBytes
      bytes must not(beEmpty)
      val recovered = KryoJsonSerialization.deserializeAndRender(new Input(bytes))
      recovered mustEqual json.replaceAll(" ", "")
    }
    "correctly de/serialize geojson" in {
      val out = new Output(768)
      val jsons = geoms.map { geom =>
        s"""{ "type": "Feature", "geometry": $geom, "properties": { "prop0": "value0", "prop1": { "this": "that" } } }"""
      }
      forall(jsons) { json =>
        out.clear()
        KryoJsonSerialization.serialize(out, json)
        val bytes = out.toBytes
        bytes must not(beEmpty)
        val recovered = KryoJsonSerialization.deserializeAndRender(new Input(bytes))
        recovered mustEqual json.replaceAll("[ \n]", "")
      }
    }
    "correctly serialize array json" in {
      val out = new Output(128)
      val jsons =
        Seq(
          """["a1","a2"]""",
          """[1,2,3]""",
          """[]"""
        )

      forall(jsons) { json =>
        out.clear()
        KryoJsonSerialization.serialize(out, json)
        val bytes = out.toBytes
        bytes must not(beEmpty)
        val recovered = KryoJsonSerialization.deserializeAndRender(new Input(bytes))
        recovered mustEqual json
      }
    }
    "correctly serialize non-document json" in {
      val out = new Output(128)
      val jsons =
        Seq(
          "\"foo\"",
          "2.1",
          "2",
          "false"
        )

      forall(jsons) { json =>
        out.clear()
        KryoJsonSerialization.serialize(out, json)
        val bytes = out.toBytes
        bytes must not(beEmpty)
        val recovered = KryoJsonSerialization.deserializeAndRender(new Input(bytes))
        recovered mustEqual json
      }
    }.pendingUntilFixed("json4s native doesn't support parsing primitives")

    "correctly deserialize json-path for documents" in {
      val out = new Output(512)
      val json =
        """{
           |  "type": "Feature",
           |  "geometry": {
           |    "type": "Point",
           |    "coordinates": [30, 10]
           |  },
           |  "properties": {
           |    "type": 20,
           |    "prop0": "value0",
           |    "prop1": {
           |      "this": "that"
           |    }
           |  }
           |}""".stripMargin
      KryoJsonSerialization.serialize(out, json)
      val bytes = out.toBytes

      deserialize(bytes, "$.foo") must beNull
      deserialize(bytes, "$.type") mustEqual "Feature"
      deserialize(bytes, "$.geometry.type") mustEqual "Point"
      deserialize(bytes, "$.geometry.*") mustEqual Seq("Point", Seq(30, 10).asJava).asJava
      deserialize(bytes, "$.geometry.coordinates") mustEqual Seq(30, 10).asJava
      deserialize(bytes, "$.geometry.coordinates[0]") mustEqual 30
      deserialize(bytes, "$.geometry.coordinates[0,1]") mustEqual Seq(30, 10).asJava
      deserialize(bytes, "$.*.type") mustEqual Seq("Point", 20).asJava
      deserialize(bytes, "$.geometry.coordinates[*]") mustEqual Seq(30, 10).asJava
      deserialize(bytes, "$.geometry.coordinates[*].length()") mustEqual 2
      deserialize(bytes, "$..type") mustEqual Seq("Feature", "Point", 20).asJava
      deserialize(bytes, "$.properties..*") mustEqual
          Seq(20, "value0", """{"this":"that"}""", "that").asJava
    }

    "correctly deserialize json-path for arrays" in {
      val out = new Output(512)
      val json = """["a1","a2"]"""
      KryoJsonSerialization.serialize(out, json)
      val bytes = out.toBytes

      deserialize(bytes, "$.foo") must beNull
      deserialize(bytes, "$[*]") mustEqual Seq("a1", "a2").asJava
      deserialize(bytes, "$[*].length()") mustEqual 2
    }

    "correctly deserialize json-path functions" in {
      val out = new Output(1024)
      KryoJsonSerialization.serialize(out, bookJson)
      val bytes = out.toBytes

      foreach(Seq("$.store.book", "$.store.book[*]")) { path =>
        deserialize(bytes, s"$path.length()") mustEqual 4
        deserialize(bytes, s"$path.first()") mustEqual books(0)
        deserialize(bytes, s"$path.last()") mustEqual books(3)
        deserialize(bytes, s"$path.index(1)") mustEqual books(1)
      }

      deserialize(bytes, "$.store.book[*].price.min()") mustEqual 8.95
      deserialize(bytes, "$.store.book[*].price.max()") mustEqual 22.99
      deserialize(bytes, "$.store.book[*].price.sum()") mustEqual 53.92
      deserialize(bytes, "$.store.book[*].price.avg()") mustEqual 13.48

      deserialize(bytes, "$..price.min()") mustEqual 8.95
      deserialize(bytes, "$..price.max()") mustEqual 22.99
      deserialize(bytes, "$..price.sum()") mustEqual 73.87
      deserialize(bytes, "$..price.avg()") match {
        case d: Double => d must beCloseTo(14.774, 0.0001)
        case a => ko(s"expected double but got: $a")
      }
    }

    "correctly deserialize json-path filters" in {
      val out = new Output(1024)
      KryoJsonSerialization.serialize(out, bookJson)
      val bytes = out.toBytes

      def test(path: String, expected: Any): MatchResult[Any] = {
        val parsed = JsonPathParser.parse(path)
        KryoJsonSerialization.deserialize(new Input(bytes), parsed) mustEqual expected
        JsonPathPropertyAccessor.evaluateJsonPath(bookJson, parsed) mustEqual expected
      }

      test("$.store.book[?(@.price == 8.95 && @.category == 'reference')]", books(0))
      test("$.store.book[?(@.price != 8.95 && @.author != 'Evelyn Waugh')]", books.drop(2).asJava)
      test("$.store.book[?(@.price > 12.99)]", books(3))
      test("$.store.book[?(@.price >= 12.99)]", Seq(books(1), books(3)).asJava)
      test("$.store.book[?(@.price < 12.99)]", Seq(books(0), books(2)).asJava)
      test("$.store.book[?(@.price <= 12.99)]", books.take(3).asJava)
      test("$.store.book[?(@.title =~ /S.*/)]", books.take(2).asJava)
      test("$.store.book[?(@.category in ['reference','fiction'])]", books.asJava)
      test("$.store.book[?(@.category nin [ 'reference', 'fiction' ])]", null)
      test("$.store[?(@.book[*].category subsetof ['reference','fiction','nonfiction'])].bicycle.color", "red")
      test("$.store[?(@.book[*].category anyof ['fiction','nonfiction'])].bicycle.color", "red")
      test("$.store[?(@.book[*].category noneof ['fiction','nonfiction'])].bicycle.color", null)
      test("$.store.book[?(@.author size @.title)]", null)
      test("$.store.book[?(@.author empty true)]", null)
      test("$.store.book[?(@.author empty false)]", books.asJava)
      test("$.store.book[?(@.isbn)]", books.drop(2).asJava)
    }

    "correctly deserialize json-path examples" in {
      val out = new Output(1024)
      KryoJsonSerialization.serialize(out, bookJson)
      val bytes = out.toBytes

      val tests = Seq(
        "$.store.book[*].author",
        "$..author",
        "$.store.*",
        "$.store..price",
        "$..book[2]",
        "$..book[-2]",
        "$..book[0,1]",
        "$..book[:2]",
        "$..book[1:2]",
        "$..book[-2:]",
        "$..book[2:]",
        "$..book[?(@.isbn)]",
        "$.store.book[?(@.price < 10)]",
        "$..book[?(@.price <= $['expensive'])]",
        "$..book[?(@.author =~ /.*REES/i)]",
        "$..book[?(!(@.price < 10 && @.category == 'fiction'))]",
        "$..book[?(@.price < 10 && @.category == 'fiction')]",
        "$..book[?(@.category == 'reference' || @.price > 10)]",
        "$..*",
      )
      foreach(tests) { test =>
        val path = JsonPathParser.parse(test)
        val expected = JsonPathPropertyAccessor.evaluateJsonPath(bookJson, path)
        val actual = KryoJsonSerialization.deserialize(new Input(bytes), path)
        actual mustEqual expected
      }

      // these tests seem like we're evaluating them correctly, but jayway is not...
      val unexpected = Seq(
        "$.store.book.length()" -> 4,
      )
      foreach(unexpected) { case (test, expected) =>
        val path = JsonPathParser.parse(test)
        // TODO if jayway starts working correctly, update this test
        JsonPathPropertyAccessor.evaluateJsonPath(bookJson, path) must not(beEqualTo(expected))
        KryoJsonSerialization.deserialize(new Input(bytes), path) mustEqual expected
      }
    }
  }

  def deserialize(in: Array[Byte], path: String): Any =
    KryoJsonSerialization.deserialize(new Input(in), JsonPathParser.parse(path))
}
