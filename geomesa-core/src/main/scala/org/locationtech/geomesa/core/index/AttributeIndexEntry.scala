/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.calrissian.mango.types.{LexiTypeEncoders, SimpleTypeEncoders, TypeEncoder}
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.core.data._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Contains logic for converting between accumulo and geotools for the attribute index
 */
object AttributeIndexEntry extends Logging {

  val typeRegistry = LexiTypeEncoders.LEXI_TYPES
  val nullString = "<null>"
  private val genericAlias = "obj"
  private val NULLBYTE = "\u0000"

  /**
   * Gets mutations for the attribute index table
   *
   * @param feature
   * @param indexedAttributes attributes that will be indexed
   * @param visibility
   * @param delete whether we are writing or deleting
   * @return
   */
  def getAttributeIndexMutations(feature: SimpleFeature,
                                 indexedAttributes: Seq[AttributeDescriptor],
                                 visibility: ColumnVisibility,
                                 delete: Boolean = false): Seq[Mutation] = {
    val cf = new Text(feature.getID)
    lazy val value = IndexEntry.encodeIndexValue(feature)
    indexedAttributes.map { descriptor =>
      val attribute = Option(feature.getAttribute(descriptor.getName))
      val m = new Mutation(getAttributeIndexRow(descriptor.getLocalName, attribute))
      if (delete) {
        m.putDelete(cf, EMPTY_COLQ, visibility)
      } else {
        m.put(cf, EMPTY_COLQ, visibility, value)
      }
      m
    }
  }

  /**
   * Gets a row key for the attribute index
   *
   * @param attributeName
   * @param attributeValue
   * @return
   */
  def getAttributeIndexRow(attributeName: String, attributeValue: Option[Any]): String =
    attributeName ++ NULLBYTE ++ encode(attributeValue)

  /**
   * Lexicographically encode the value
   *
   * @param valueOption
   * @return
   */
  def encode(valueOption: Option[Any]): String = {
    val value = valueOption.getOrElse(nullString)
    Try(typeRegistry.encode(value)).map(typeRegistry.getAlias(value) ++ NULLBYTE ++ _) match {
      case Success(encoded) => encoded
      case Failure(e) => genericAlias ++ NULLBYTE ++ value.toString
    }
  }

  private val dateFormat = ISODateTimeFormat.dateTime();
  private val simpleEncoders = SimpleTypeEncoders.SIMPLE_TYPES.getAllEncoders.asScala

  private type TryEncoder = Try[(TypeEncoder[Any, String], TypeEncoder[_, String])]

  /**
   * Tries to convert a value from one class to another. When querying attributes, the query
   * literal has to match the class of the attribute for lexicoding to work.
   *
   * @param value
   * @param current
   * @param desired
   * @return
   */
  def convertType(value: Any, current: Class[_], desired: Class[_]): Any = {
    val result =
      if (current == desired) {
        Success(value)
      } else if (desired == classOf[Date] && current == classOf[String]) {
        // try to parse the string as a date - right now we support just ISO format
        Try(dateFormat.parseDateTime(value.asInstanceOf[String]).toDate)
      } else {
        // cheap way to convert between basic classes (string, int, double, etc) - encode the value
        // to a string and then decode to the desired class
        val encoderOpt = simpleEncoders.find(_.resolves().equals(current)).map(_.asInstanceOf[TypeEncoder[Any, String]])
        val decoderOpt = simpleEncoders.find(_.resolves().equals(desired))
        (encoderOpt, decoderOpt) match {
          case (Some(e), Some(d)) => Try(d.decode(e.encode(value)))
          case _ => Failure(new RuntimeException("No matching encoder/decoder"))
        }
      }

    result match {
      case Success(converted) => converted
      case Failure(e) =>
        logger.warn(s"Error converting type for '$value' from ${current.getSimpleName} to " +
                    s"${desired.getSimpleName}: ${e.toString}")
        value
    }
  }
}
