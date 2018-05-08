/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.io.InputStream
import java.{lang => jl}
import java.time.format.DateTimeParseException

import com.vividsolutions.jts.geom._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.core.JsonToken._
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate
import org.locationtech.geomesa.utils.geotools.{AttributeSpec, CRS_EPSG_4326}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureSpecConfig.TypeNamePath
import org.locationtech.geomesa.utils.text.DateParsing

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GeoJsonInference {

  val FieldsPath = "fields"
  val OptionsPath = "options"

  val attributeBuilder = new AttributeTypeBuilder()
  val maxToRead = 10

  val baseConfig: Config = ConfigFactory.empty()
    .withValue("type", ConfigValueFactory.fromAnyRef("json"))
    .withValue("feature-path", ConfigValueFactory.fromAnyRef("$[*]"))
    .withValue("options", ConfigValueFactory.fromMap(Map(("line-mode", "multi"))))

  def inferSft(filePath: String, sftName: String): SimpleFeatureType = {
    val fileHandle: FileSystemDelegate.FileHandle = PathUtils.interpretPath(filePath).head
    val builder = WithClose(fileHandle.open)(inferSftBuilder)
    builder.setName(sftName)
    builder.buildFeatureType()
  }

  def inferSft(is: InputStream, sftName: String = "geojson"): SimpleFeatureType = {
    val builder = inferSftBuilder(is)
    builder.setName(sftName)
    builder.buildFeatureType()
  }

  def inferSftBuilder(is: InputStream): SimpleFeatureTypeBuilder = {
    val sftBuilder = new SimpleFeatureTypeBuilder()
    val factory = new JsonFactory()
    val parser = factory.createParser(is)
    parser.nextToken()

    val attributes: mutable.Map[String, AttributeDescriptor] = mutable.Map()
    val geomTypes: ArrayBuffer[String] = ArrayBuffer()

    var numRead = 0
    while (parser.hasCurrentToken && numRead < maxToRead) {

      val name = parser.getCurrentName
      if (name != null && name.equals("geometry")) {
        geomTypes += getKeyValue(parser, "type")
        // advance to end of geom object
        while (nextUntil(parser, END_OBJECT)) { }
      } else if (name != null && name.equals("properties")) {
        while(nextUntil(parser, START_OBJECT)) { }
        parser.nextToken()

        // parse the properties
        val properties: ArrayBuffer[AttributeDescriptor] = ArrayBuffer()
        while (parser.hasCurrentToken && !parser.getCurrentToken.isStructEnd) {
          val prop = inferField(parser)
          properties +=  prop
          parser.nextToken()
        }
        // merge any conflicts
        val propMap = properties.map{ a => (a.getLocalName, a) }.toMap
        mergeMaps(attributes, propMap)
        numRead += 1
      }
      parser.nextToken()
    }

    // check valid
    if (attributes.isEmpty) {
      throw new UnsupportedOperationException("No SFT was provided and none could not be inferred. " +
        "Provide one or ensure the data is valid geojson.")
    }

    // add geometry
    val mergedGeomType = mergeGeomTypes(geomTypes)
    val allAttrs = attributes.values.toArray :+ buildGeomAttr(mergedGeomType)

    sftBuilder.addAll(allAttrs)
    sftBuilder.setDefaultGeometry("geom")
    sftBuilder
  }

  def inferConfig(sft: SimpleFeatureType, idFieldName: Option[String] = None): Config = {
    val attributes = sft.getAttributeDescriptors.map { ad =>
      val config = AttributeSpec(sft, ad).toConfigMap
      val attrMap: java.util.HashMap[String,String] = new java.util.HashMap[String,String](config.asJava)
      val typeVal = attrMap.get("type").toLowerCase()
      ad.getType.getBinding match {
        case t if classOf[java.util.List[_]].isAssignableFrom(t) =>
          attrMap.put("json-type", "list")
          attrMap.put("transform", "jsonList('string', $0)")
          attrMap.put("path", "$.properties." + attrMap.get("name"))
        case g if classOf[Geometry].isAssignableFrom(g) =>
          val geomType = attrMap.get("type").toLowerCase()
          attrMap.put("json-type", "geometry")
          attrMap.put("transform", s"$geomType($$0)")
          attrMap.put("path", "$.geometry")
        case _ =>
          attrMap.put("json-type", typeVal)
          attrMap.put("path", "$.properties." + attrMap.get("name"))
      }

      attrMap.put("name", attrMap.get("name"))
      attrMap
    }

    baseConfig
      .withValue(TypeNamePath, ConfigValueFactory.fromAnyRef(sft.getTypeName))
      .withValue(FieldsPath, ConfigValueFactory.fromIterable(attributes))
      .withValue("id-field", ConfigValueFactory.fromAnyRef("md5(stringToBytes(jsonToString($0)))"))
  }

  def mergeMaps(attrsA: mutable.Map[String, AttributeDescriptor],
                attrsB: Map[String, AttributeDescriptor]): Unit = {
    val (overlappingKeys, disjointKeys) = attrsB.keys.partition(attrsA.contains)

    // Add fields unique to B
    disjointKeys.foreach{ name => attrsA.put(name, attrsB(name)) }
    // Merge overlaps
    val merged = overlappingKeys.map { name =>
      val bindingA = attrsA(name).getType.getBinding
      val bindingB = attrsB(name).getType.getBinding
      if (bindingB == classOf[jl.Object] || bindingA == bindingB) {
        (name, attrsA(name))
      } else if (bindingA == classOf[jl.Object]) {
        (name, attrsB(name))
      } else {
        val JDouble = classOf[jl.Double]
        val JInteger = classOf[jl.Integer]
        val JLong = classOf[jl.Long]
        (bindingA, bindingB) match {
          case (JDouble, JInteger) => (name, attrsA(name))
          case (JInteger, JDouble) => (name, attrsB(name))
          case (JLong, JInteger) => (name, attrsA(name))
          case (JInteger, JLong) => (name, attrsB(name))
          case (JDouble, JLong) => (name, attrsA(name))
          case (JLong, JDouble) => (name, attrsB(name))
          case _ => (name, attrsA(name))
        }
      }
    }
    attrsA ++= merged
  }

  def mergeGeomTypes(geomTypes: ArrayBuffer[String]): String = {
    val firstNonNull = geomTypes.find(t => t != null)
    firstNonNull match {
      case Some(geomType) =>
        if (geomTypes.forall(_.equals(geomType))) {
          geomType
        } else {
          "Geometry"
        }
      case None => throw new UnsupportedOperationException("Could not locate geometry in geojson file.")
    }
  }

  // NB: This method is adapted from Apache Spark's JsonInferSchema class
  /*
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *    http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
  private def inferField(parser: JsonParser): AttributeDescriptor = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.getCurrentToken match {
      case null | VALUE_NULL =>
        buildAttr(classOf[jl.Object], parser.getCurrentName)

      case FIELD_NAME =>
        parser.nextToken()
        inferField(parser)

      case VALUE_STRING if parser.getTextLength < 1 =>
        // Zero length strings and nulls have special handling to deal
        // with JSON generators that do not distinguish between the two.
        // To accurately infer types for empty strings that are really
        // meant to represent nulls we assume that the two are isomorphic
        // but will defer treating null fields as strings until all the
        // record fields' types have been combined.
        buildAttr(classOf[jl.Object], parser.getCurrentName)

      case VALUE_STRING =>
        val name = parser.getCurrentName
        val text = parser.getText
        try {
          DateParsing.parse(text)
          buildAttr(classOf[java.util.Date], name)
        } catch {
          case _: DateTimeParseException => buildAttr(classOf[jl.String], name)
        }

      case START_OBJECT =>
        val name = parser.getCurrentName
        // exhaust object
        val builder = new StringBuilder
        while (nextUntil(parser, END_OBJECT)) { builder.append(parser.getText) }
        buildAttr(classOf[jl.String], name)

      case END_OBJECT =>
        parser.nextToken()
        inferField(parser)

      case START_ARRAY =>
        val name = parser.getCurrentName
        // exhaust array
        val builder = new StringBuilder
        while (nextUntil(parser, END_ARRAY)) { builder.append(parser.getText) }
        buildAttr(classOf[java.util.List[jl.String]], name)

      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        import JsonParser.NumberType._
        parser.getNumberType match {
          case INT => buildAttr(classOf[jl.Integer], parser.getCurrentName)
          case LONG => buildAttr(classOf[jl.Long], parser.getCurrentName)
          // Anything more precise than Long becomes a Double
          case BIG_INTEGER | BIG_DECIMAL =>  buildAttr(classOf[jl.Double], parser.getCurrentName)
          case FLOAT | DOUBLE => buildAttr(classOf[jl.Double], parser.getCurrentName)
        }

      case VALUE_TRUE | VALUE_FALSE => buildAttr(classOf[jl.Boolean], parser.getCurrentName)
    }
  }

  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    val nextTok = parser.nextToken()
    nextTok match {
      case null => false
      case x => x != stopOn
      case _ => false
    }
  }

  def reachedKey(parser: JsonParser, stopOn: String): Boolean = {
    val name = parser.getCurrentName
    if (parser.getCurrentToken == null) {
      true
    } else if (name != null && name.equals(stopOn)) {
      true
    } else {
      false
    }
  }

  def getKeyValue(parser: JsonParser, key: String): String = {
    while (!reachedKey(parser, key)) { parser.nextToken() }
    parser.nextTextValue()
  }

  def buildAttr[T: ClassTag](clazz: Class[T], name: String) : AttributeDescriptor = {
    attributeBuilder.setBinding(clazz)
    attributeBuilder.setName(name)
    val nameType = attributeBuilder.buildType()
    attributeBuilder.buildDescriptor(name, nameType)
  }

  def buildGeomAttr(geomType: String): AttributeDescriptor = {
    geomType match {
      case "Geometry" => buildGeomAttr(classOf[Geometry])
      case "Point" => buildGeomAttr(classOf[Point])
      case "LineString" => buildGeomAttr(classOf[LineString])
      case "Polygon" => buildGeomAttr(classOf[Polygon])
      case "MultiPoint" => buildGeomAttr(classOf[MultiPoint])
      case "MultiLineString" => buildGeomAttr(classOf[MultiLineString])
      case "MultiPolygon" => buildGeomAttr(classOf[MultiPolygon])
      case "GeometryCollection" => buildGeomAttr(classOf[GeometryCollection])
    }
  }

  def buildGeomAttr[T: ClassTag](clazz: Class[T]) : AttributeDescriptor = {
    attributeBuilder.setBinding(clazz)
    attributeBuilder.setName("geom")
    attributeBuilder.userData("default", "true")
    attributeBuilder.crs(CRS_EPSG_4326)
    val nameType = attributeBuilder.buildGeometryType()
    attributeBuilder.buildDescriptor("geom", nameType)
  }


}