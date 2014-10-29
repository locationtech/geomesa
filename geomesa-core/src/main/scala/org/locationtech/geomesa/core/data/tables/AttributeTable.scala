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


package org.locationtech.geomesa.core.data.tables

import java.util.{Date, Locale, Collection => JCollection, Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.calrissian.mango.types.{LexiTypeEncoders, SimpleTypeEncoders, TypeEncoder}
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.IndexEntry
import org.locationtech.geomesa.utils.geotools.Conversions.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Contains logic for converting between accumulo and geotools for the attribute index
 */
object AttributeTable extends GeoMesaTable with Logging {
  /** Creates a function to write a feature to the attribute index **/
  def attrWriter(bw: BatchWriter,
                 indexedAttributes: Seq[AttributeDescriptor],
                 visibility: String,
                 rowIdPrefix: String): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val mutations = getAttributeIndexMutations(feature,
        indexedAttributes,
        new ColumnVisibility(visibility),
        rowIdPrefix)
      bw.addMutations(mutations)
    }

  /** Creates a function to remove attribute index entries for a feature **/
  def removeAttrIdx(bw: BatchWriter,
                    indexedAttributes: Seq[AttributeDescriptor],
                    visibility: String,
                    rowIdPrefix: String): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val mutations = getAttributeIndexMutations(feature,
        indexedAttributes,
        new ColumnVisibility(visibility),
        rowIdPrefix,
        true)
      bw.addMutations(mutations)
    }

  val typeRegistry = LexiTypeEncoders.LEXI_TYPES
  val nullString = ""
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
                                 rowIdPrefix: String,
                                 delete: Boolean = false): Seq[Mutation] = {
    val cq = new Text(feature.getID)
    lazy val value = IndexEntry.encodeIndexValue(feature)
    indexedAttributes.flatMap { descriptor =>
      val attribute = Option(feature.getAttribute(descriptor.getName))
      val mutations = getAttributeIndexRows(rowIdPrefix, descriptor, attribute).map(new Mutation(_))
      if (delete) {
        mutations.foreach(_.putDelete(EMPTY_COLF, cq, visibility))
      } else {
        mutations.foreach(_.put(EMPTY_COLF, cq, visibility, value))
      }
      mutations
    }
  }

  /**
   * Gets row keys for the attribute index. Usually a single row will be returned, but collections
   * will result in multiple rows.
   *
   * @param rowIdPrefix
   * @param descriptor
   * @param value
   * @return
   */
  def getAttributeIndexRows(rowIdPrefix: String,
                            descriptor: AttributeDescriptor,
                            value: Option[Any]): Seq[String] = {
    val prefix = getAttributeIndexRowPrefix(rowIdPrefix, descriptor)
    encode(value, descriptor).map(prefix + _)
  }

  /**
   * Gets a prefix for an attribute row - useful for ranges over a particular attribute
   *
   * @param rowIdPrefix
   * @param descriptor
   * @return
   */
  def getAttributeIndexRowPrefix(rowIdPrefix: String, descriptor: AttributeDescriptor): String =
    rowIdPrefix ++ descriptor.getLocalName ++ NULLBYTE

  /**
   * Decodes an attribute value out of row string
   *
   * @param rowIdPrefix table sharing prefix
   * @param descriptor the attribute we're decoding
   * @param row
   * @return
   */
  def decodeAttributeIndexRow(rowIdPrefix: String,
                              descriptor: AttributeDescriptor,
                              row: String): Try[AttributeIndexRow] =
    for {
      suffix <- Try(row.substring(rowIdPrefix.length))
      separator = suffix.indexOf(NULLBYTE)
      name = suffix.substring(0, separator)
      encodedValue = suffix.substring(separator + 1)
      decodedValue = decode(encodedValue, descriptor)
    } yield {
      AttributeIndexRow(name, decodedValue)
    }

  /**
   * Lexicographically encode the value. Collections will return multiple rows, one for each entry.
   *
   * @param valueOption
   * @param descriptor
   * @return
   */
  def encode(valueOption: Option[Any], descriptor: AttributeDescriptor): Seq[String] = {
    val value = valueOption.getOrElse(nullString)
    if (descriptor.isCollection) {
      // encode each value into a separate row
      value.asInstanceOf[JCollection[_]].asScala.toSeq.map(Option(_).getOrElse(nullString)).map(typeEncode)
    } else if (descriptor.isMap) {
      // TODO GEOMESA-454 - support querying against map attributes
      Seq.empty
    } else {
      Seq(typeEncode(value))
    }
  }

  private def typeEncode(value: Any): String = Try(typeRegistry.encode(value)).getOrElse(value.toString)

  /**
   * Decode an encoded value. Note that for collection types, only a single entry of the collection
   * will be decoded - this is because the collection entries have been broken up into multiple rows.
   *
   * @param encoded
   * @param descriptor
   * @return
   */
  def decode(encoded: String, descriptor: AttributeDescriptor): Any = {
    if (descriptor.isCollection) {
      // get the alias from the type of values in the collection
      val alias = SimpleFeatureTypes.getCollectionType(descriptor).map(_.getSimpleName.toLowerCase(Locale.US)).head
      Seq(typeRegistry.decode(alias, encoded)).asJava
    } else if (descriptor.isMap) {
      // TODO GEOMESA-454 - support querying against map attributes
      Map.empty.asJava
    } else {
      val alias = descriptor.getType.getBinding.getSimpleName.toLowerCase(Locale.US)
      typeRegistry.decode(alias, encoded)
    }
  }

  private val dateFormat = ISODateTimeFormat.dateTime()
  private val simpleEncoders = SimpleTypeEncoders.SIMPLE_TYPES.getAllEncoders

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

case class AttributeIndexRow(attributeName: String, attributeValue: Any)
