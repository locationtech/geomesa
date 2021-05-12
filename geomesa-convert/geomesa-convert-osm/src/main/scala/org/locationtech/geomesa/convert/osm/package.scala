/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import de.topobyte.osm4j.core.model.iface.{EntityContainer, OsmEntity}
import de.topobyte.osm4j.core.model.util.OsmModelUtil
import de.topobyte.osm4j.pbf.seq.PbfIterator
import de.topobyte.osm4j.xml.dynsax.OsmXmlIterator
import org.locationtech.geomesa.convert.osm.OsmAttribute.OsmAttribute
import org.locationtech.geomesa.convert.osm.OsmFormat.OsmFormat
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.jts.geom.Geometry
import pureconfig.ConfigObjectCursor
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

import java.io.InputStream
import java.util.{Date, Locale}
import scala.util.control.NonFatal

package object osm {

  import scala.collection.JavaConverters._

  /**
   * Determines if the fields require parsing OSM entity metadata or not
   *
   * @param fields fields
   * @return
   */
  def requiresMetadata(fields: Seq[org.locationtech.geomesa.convert2.Field]): Boolean =
    fields.exists { case AttributeField(_, tag, _) => OsmAttribute.requiresMetadata(tag); case _ => false }

  /**
   * Create an iterator over OSM entities
   *
   * @param is input stream
   * @param format OSM format
   * @param fetchMetadata read metadata values or skip them
   * @return
   */
  def osmIterator(is: InputStream, format: OsmFormat, fetchMetadata: Boolean): CloseableIterator[EntityContainer] = {
    val iter: java.util.Iterator[EntityContainer] = format match {
      case OsmFormat.pbf => new PbfIterator(is, fetchMetadata)
      case OsmFormat.xml => new OsmXmlIterator(is, fetchMetadata)
    }
    CloseableIterator(iter.asScala, is.close())
  }

  /**
   * Convert an entity to an array of values, including entity metadata
   *
   * @param entity entity
   * @param geometry geometry
   * @return
   */
  def toArrayWithMetadata(entity: OsmEntity, geometry: Geometry): Array[Any] = {
    val tags = OsmModelUtil.getTagsAsMap(entity)
    val metadata = Option(entity.getMetadata)
    val timestamp = metadata.map(m => new Date(m.getTimestamp)).orNull
    val user = metadata.map(_.getUser).orNull
    val uid = metadata.map(m => Long.box(m.getUid)).orNull
    val version = metadata.map(m => Int.box(m.getVersion)).orNull
    val changeset = metadata.map(m => Long.box(m.getChangeset)).orNull
    Array(entity.getId, geometry, tags, timestamp, user, uid, version, changeset)
  }

  /**
   * Convert an entity to an array of values, ignoring entity metadata
   *
   * @param entity entity
   * @param geometry geometry
   * @return
   */
  def toArrayNoMetadata(entity: OsmEntity, geometry: Geometry): Array[Any] =
    Array(entity.getId, geometry, OsmModelUtil.getTagsAsMap(entity))

  sealed trait OsmField extends org.locationtech.geomesa.convert2.Field

  /**
   * Field referencing one of the OSM attributes
   *
   * @param name field name
   * @param attribute attribute
   * @param transforms transforms
   */
  case class AttributeField(name: String, attribute: OsmAttribute, transforms: Option[Expression]) extends OsmField {

    private val lookup = OsmAttribute.index(attribute)

    override val fieldArg: Option[Array[AnyRef] => AnyRef] = Some(values)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
      transforms match {
        case None => args(lookup)
        case Some(t) => mutableArray(0) = args(lookup); t.eval(mutableArray)
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
    private def values(args: Array[AnyRef]): AnyRef = args(lookup)
  }

  /**
   * Field Derived from other fields, with no OSM-specific values
   *
   * @param name field name
   * @param transforms transforms
   */
  case class DerivedField(name: String, transforms: Option[Expression]) extends OsmField {
    override val fieldArg: Option[Array[AnyRef] => AnyRef] = None
  }

  /**
   * OSM file formats
   */
  object OsmFormat extends Enumeration {
    type OsmFormat = Value
    val pbf, xml = Value
  }

  /**
   * Enumeration of the attributes stored with each OSM field
   */
  object OsmAttribute extends Enumeration {

    type OsmAttribute = Value
    val id, geometry, tags, timestamp, user, uid, version, changeset = Value

    def requiresMetadata(attribute: OsmAttribute): Boolean = attribute match {
      case OsmAttribute.user      => true
      case OsmAttribute.uid       => true
      case OsmAttribute.version   => true
      case OsmAttribute.changeset => true
      case OsmAttribute.timestamp => true
      case _                      => false
    }

    def index(attribute: OsmAttribute): Int = attribute match {
      case OsmAttribute.id        => 0
      case OsmAttribute.geometry  => 1
      case OsmAttribute.tags      => 2
      case OsmAttribute.timestamp => 3
      case OsmAttribute.user      => 4
      case OsmAttribute.uid       => 5
      case OsmAttribute.version   => 6
      case OsmAttribute.changeset => 7
    }
  }

  object OsmFieldConvert extends FieldConvert[OsmField] with OptionConvert {
    override protected def decodeField(
        cur: ConfigObjectCursor,
        name: String,
        transform: Option[Expression]): Either[ConfigReaderFailures, OsmField] = {
      optional(cur, "attribute").right.flatMap {
        case None => Right(DerivedField(name, transform))
        case Some(a) =>
          try { Right(AttributeField(name, OsmAttribute.withName(a.toLowerCase(Locale.US)), transform)) } catch {
            case NonFatal(_) =>
              val msg = s"Not a valid OSM field attribute. Valid values are: '${OsmAttribute.values.mkString("', '")}'"
              cur.failed(CannotConvert(a, "OsmField", msg))
          }
      }
    }

    override protected def encodeField(field: OsmField, base: java.util.Map[String, AnyRef]): Unit = {
      field match {
        case f: AttributeField => base.put("attribute", f.attribute.toString)
        case _ => // no-op
      }
    }
  }
}
