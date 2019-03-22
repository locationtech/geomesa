/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.Config
import org.apache.commons.text.StringEscapeUtils
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.utils.geotools.NameableFeatureTypeFactory.NameableSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.GeomAttributeSpec
import org.locationtech.geomesa.utils.geotools.sft._
import org.opengis.feature.`type`.{AttributeDescriptor, FeatureTypeFactory, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType
import org.parboiled.errors.ParsingException

object SimpleFeatureTypes {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  object Configs {
    val TABLE_SHARING_KEY    = "geomesa.table.sharing"
    val DEFAULT_DATE_KEY     = "geomesa.index.dtg"
    val IGNORE_INDEX_DTG     = "geomesa.ignore.dtg"
    val VIS_LEVEL_KEY        = "geomesa.visibility.level"
    val Z3_INTERVAL_KEY      = "geomesa.z3.interval"
    val XZ_PRECISION_KEY     = "geomesa.xz.precision"
    val TABLE_SPLITTER       = "table.splitter.class" // note: doesn't start with geomesa so we don't persist it
    val TABLE_SPLITTER_OPTS  = "table.splitter.options"
    val MIXED_GEOMETRIES     = "geomesa.mixed.geometries"
    val RESERVED_WORDS       = "override.reserved.words" // note: doesn't start with geomesa so we don't persist it
    val DEFAULT_DTG_JOIN     = "override.index.dtg.join"
    val KEYWORDS_KEY         = "geomesa.keywords"
    val ENABLED_INDICES      = "geomesa.indices.enabled"
    // keep around old values for back compatibility
    val ENABLED_INDEX_OPTS   = Seq(ENABLED_INDICES, "geomesa.indexes.enabled", "table.indexes.enabled")
    @deprecated("GeoHash index is no longer supported")
    val ST_INDEX_SCHEMA_KEY  = "geomesa.index.st.schema"
    val Z_SPLITS_KEY         = "geomesa.z.splits"
    val ATTR_SPLITS_KEY      = "geomesa.attr.splits"
    val ID_SPLITS_KEY        = "geomesa.id.splits"
    val LOGICAL_TIME_KEY     = "geomesa.logical.time"
    val COMPRESSION_ENABLED  = "geomesa.table.compression.enabled"
    val COMPRESSION_TYPE     = "geomesa.table.compression.type"  // valid: snappy, lzo, gz(default), bzip2, lz4, zstd
    val FID_UUID_KEY         = "geomesa.fid.uuid"
    val FID_UUID_ENCODED_KEY = "geomesa.fid.uuid-encoded"
    val TABLE_PARTITIONING   = "geomesa.table.partition"
    val QUERY_INTERCEPTORS   = "geomesa.query.interceptors"
  }

  private [geomesa] object InternalConfigs {
    val GEOMESA_PREFIX          = "geomesa."
    val SHARING_PREFIX_KEY      = "geomesa.table.sharing.prefix"
    val USER_DATA_PREFIX        = "geomesa.user-data.prefix"
    val INDEX_VERSIONS          = "geomesa.indices"
    val PARTITION_SPLITTER      = "geomesa.splitter.class"
    val PARTITION_SPLITTER_OPTS = "geomesa.splitter.opts"
    val REMOTE_VERSION          = "gm.remote.version" // note: doesn't start with geomesa so we don't persist it
    val KEYWORDS_DELIMITER      = "\u0000"
  }

  object AttributeOptions {
    val OPT_DEFAULT      = "default"
    val OPT_SRID         = "srid"
    val OPT_INDEX_VALUE  = "index-value"
    val OPT_INDEX        = "index"
    val OPT_STATS        = "keep-stats"
    val OPT_CARDINALITY  = "cardinality"
    val OPT_COL_GROUPS   = "column-groups"
    val OPT_CQ_INDEX     = "cq-index"
    val OPT_JSON         = "json"
    val OPT_PRECISION    = "precision"
  }

  private [geomesa] object AttributeConfigs {
    val USER_DATA_LIST_TYPE      = "subtype"
    val USER_DATA_MAP_KEY_TYPE   = "keyclass"
    val USER_DATA_MAP_VALUE_TYPE = "valueclass"
  }

  private val cache = new ConcurrentHashMap[(String, String), ImmutableSimpleFeatureType]()

  /**
    * Create a simple feature type from a specification. Extends DataUtilities.createType with
    * GeoMesa-specific functionality like list/map attributes, indexing options, etc.
    *
    * @param typeName type name - may include namespace
    * @param spec specification
    * @return
    */
  def createType(typeName: String, spec: String): SimpleFeatureType = {
    val (namespace, name) = parseTypeName(typeName)
    createType(namespace, name, spec)
  }

  /**
    * Create a simple feature type from a specification. Extends DataUtilities.createType with
    * GeoMesa-specific functionality like list/map attributes, indexing options, etc.
    *
    * @param namespace namespace
    * @param name name
    * @param spec specification
    * @return
    */
  def createType(namespace: String, name: String, spec: String): SimpleFeatureType = {
    val parsed = try { SimpleFeatureSpecParser.parse(spec) } catch {
      case e: ParsingException => throw new IllegalArgumentException(e.getMessage, e)
    }
    createFeatureType(namespace, name, parsed)
  }

  /**
    * Create a simple feature type from a specification. Extends DataUtilities.createType with
    * GeoMesa-specific functionality like list/map attributes, indexing options, etc.
    *
    * This method can be more efficient that `createType`, as it will return a cached, immutable instance
    *
    * Note that immutable types will never be `equals` to a mutable type, due to specific class checking
    * in the geotools implementation
    *
    * @param typeName type name - may include namespace
    * @param spec specification
    * @return
    */
  def createImmutableType(typeName: String, spec: String): SimpleFeatureType = {
    var sft = cache.get((typeName, spec))
    if (sft == null) {
      sft = immutable(createType(typeName, spec)).asInstanceOf[ImmutableSimpleFeatureType]
      cache.put((typeName, spec), sft)
    }
    sft
  }

  /**
    * Create a simple feature type from a specification. Extends DataUtilities.createType with
    * GeoMesa-specific functionality like list/map attributes, indexing options, etc.
    *
    * This method can be more efficient that `createType`, as it will return a cached, immutable instance
    *
    * Note that immutable types will never be `equals` to a mutable type, due to specific class checking
    * in the geotools implementation
    *
    * @param namespace namespace
    * @param name name
    * @param spec specification
    * @return
    */
  def createImmutableType(namespace: String, name: String, spec: String): SimpleFeatureType =
    createImmutableType(if (namespace == null) { name } else { s"$namespace:$name" }, spec)

  /**
    * Parse a SimpleFeatureType spec from a typesafe Config
    *
    * @param conf config
    * @param typeName optional typename to use for the sft. will be overridden if the config contains a type-name key
    * @param path optional config path to parse. defaults to 'sft'
    * @return
    */
  def createType(conf: Config,
                 typeName: Option[String] = None,
                 path: Option[String] = Some("sft")): SimpleFeatureType = {
    val (nameFromConf, spec) = SimpleFeatureSpecConfig.parse(conf, path)
    val (namespace, name) = parseTypeName(nameFromConf.orElse(typeName).getOrElse {
      throw new IllegalArgumentException("Unable to parse type name from provided argument or config")
    })
    createFeatureType(namespace, name, spec)
  }

  /**
    * Creates a type that can be renamed
    *
    * @param spec spec
    * @return
    */
  def createNameableType(spec: String): NameableSimpleFeatureType = {
    val parsed = try { SimpleFeatureSpecParser.parse(spec) } catch {
      case e: ParsingException => throw new IllegalArgumentException(e.getMessage, e)
    }
    createFeatureType(null, "", parsed, Some(new NameableFeatureTypeFactory())).asInstanceOf[NameableSimpleFeatureType]
  }

  /**
    * Create a single attribute descriptor
    *
    * @param spec attribute spec, e.g. 'foo:String'
    * @return
    */
  def createDescriptor(spec: String): AttributeDescriptor = {
    try { SimpleFeatureSpecParser.parseAttribute(spec).toDescriptor } catch {
      case e: ParsingException => throw new IllegalArgumentException(e.getMessage, e)
    }
  }

  /**
    * Encode a SimpleFeatureType as a comma-separated String
    *
    * @param sft - SimpleFeatureType to encode
    * @return a string representing a serialization of the sft
    */
  def encodeType(sft: SimpleFeatureType): String =
    sft.getAttributeDescriptors.asScala.map(encodeDescriptor(sft, _)).mkString(",")

  /**
    * Encode a SimpleFeatureType as a comma-separated String
    *
    * @param sft - SimpleFeatureType to encode
    * @param includeUserData - defaults to false
    * @return a string representing a serialization of the sft
    */
  def encodeType(sft: SimpleFeatureType, includeUserData: Boolean): String =
    if (includeUserData) { s"${encodeType(sft)}${encodeUserData(sft)}" } else { encodeType(sft) }

  def encodeDescriptor(sft: SimpleFeatureType, descriptor: AttributeDescriptor): String =
    SimpleFeatureSpec.attribute(sft, descriptor).toSpec

  def encodeUserData(sft: SimpleFeatureType): String = {
    val prefixes = sft.getUserDataPrefixes
    val result = new StringBuilder(";")
    sft.getUserData.asScala.foreach { case (k, v) =>
      if (v != null && prefixes.exists(k.toString.startsWith)) {
        result.append(encodeUserData(k, v)).append(",")
      }
    }
    if (result.lengthCompare(1) > 0) { result.substring(0, result.length - 1) } else { "" }
  }

  def encodeUserData(data: java.util.Map[AnyRef, AnyRef]): String = {
    if (data.isEmpty) { "" } else {
      val result = new StringBuilder(";")
      data.asScala.foreach { case (k, v) =>
        result.append(encodeUserData(k, v)).append(",")
      }
      result.substring(0, result.length - 1)
    }
  }

  def encodeUserData(key: AnyRef, value: AnyRef): String = s"$key='${StringEscapeUtils.escapeJava(value.toString)}'"

  def toConfig(sft: SimpleFeatureType, includeUserData: Boolean = true, includePrefix: Boolean = true): Config =
    SimpleFeatureSpecConfig.toConfig(sft, includeUserData, includePrefix)

  def toConfigString(sft: SimpleFeatureType,
                     includeUserData: Boolean = true,
                     concise: Boolean = false,
                     includePrefix: Boolean = true,
                     json: Boolean = false): String =
    SimpleFeatureSpecConfig.toConfigString(sft, includeUserData, concise, includePrefix, json)

  /**
    * Creates an immutable copy of the simple feature type
    *
    * Note that immutable types will never be `equals` to a mutable type, due to specific class checking
    * in the geotools implementation
    *
    * Note that some parts of the feature type may still be mutable - in particular AttributeType,
    * GeometryType and SuperType are not used by geomesa so we don't bother with them. In addition,
    * user data keys and values may be mutable objects, so while the user data map will not change,
    * the values inside may
    *
    * @param sft simple feature type
    * @param extraData additional user data to add to the simple feature type
    * @return immutable copy of the simple feature type
    */
  def immutable(sft: SimpleFeatureType, extraData: java.util.Map[AnyRef, AnyRef] = null): SimpleFeatureType = {
    sft match {
      case immutable: ImmutableSimpleFeatureType if extraData == null || extraData.isEmpty => immutable
      case _ =>
        val schema = new java.util.ArrayList[AttributeDescriptor](sft.getAttributeCount)
        var geom: GeometryDescriptor = null
        var i = 0
        while (i < sft.getAttributeCount) {
          sft.getDescriptor(i) match {
            case gd: GeometryDescriptor =>
              val descriptor = new ImmutableGeometryDescriptor(gd.getType, gd.getName, gd.getMinOccurs,
                gd.getMaxOccurs, gd.isNillable, gd.getDefaultValue, gd.getUserData)
              if (gd == sft.getGeometryDescriptor) {
                geom = descriptor
              }
              schema.add(descriptor)

            case ad =>
              schema.add(new ImmutableAttributeDescriptor(ad.getType, ad.getName, ad.getMinOccurs, ad.getMaxOccurs,
                ad.isNillable, ad.getDefaultValue, ad.getUserData))
          }
          i += 1
        }
        val userData = Option(extraData).filterNot(_.isEmpty).map { data =>
          val map = new java.util.HashMap[AnyRef, AnyRef](data.size() + sft.getUserData.size)
          map.putAll(sft.getUserData)
          map.putAll(data)
          map
        }

        new ImmutableSimpleFeatureType(sft.getName, schema, geom, sft.isAbstract, sft.getRestrictions, sft.getSuper,
          sft.getDescription, userData.getOrElse(sft.getUserData))
    }
  }

  /**
    * Creates a mutable copy of a simple feature type. If the feature type is already mutable,
    * it is returned as is
    *
    * @param sft simple feature type
    * @return
    */
  def mutable(sft: SimpleFeatureType): SimpleFeatureType = {
    if (sft.isInstanceOf[ImmutableSimpleFeatureType] || sft.getAttributeDescriptors.asScala.exists(isImmutable)) {
      // note: SimpleFeatureTypeBuilder copies attribute user data but not feature user data
      val copy = SimpleFeatureTypeBuilder.copy(sft)
      copy.getUserData.putAll(sft.getUserData)
      copy
    } else {
      sft
    }
  }

  private def isImmutable(d: AttributeDescriptor): Boolean =
    d.isInstanceOf[ImmutableAttributeDescriptor] || d.isInstanceOf[ImmutableGeometryDescriptor]

  /**
    * Renames a simple feature type. Preserves user data
    *
    * @param sft simple feature type
    * @param newName new name
    * @return
    */
  def renameSft(sft: SimpleFeatureType, newName: String): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    newName.indexOf(':') match {
      case -1 => builder.setName(newName)
      case i  => builder.setNamespaceURI(newName.substring(0, i)); builder.setName(newName.substring(i + 1))
    }
    val renamed = builder.buildFeatureType()
    renamed.getUserData.putAll(sft.getUserData)
    renamed
  }

  private def createFeatureType(namespace: String,
                                name: String,
                                spec: SimpleFeatureSpec,
                                factory: Option[FeatureTypeFactory] = None): SimpleFeatureType = {
    import AttributeOptions.OPT_DEFAULT
    import Configs.{DEFAULT_DATE_KEY, IGNORE_INDEX_DTG}

    val defaultGeom = {
      val geomAttributes = spec.attributes.collect { case g: GeomAttributeSpec => g }
      geomAttributes.find(_.options.get(OPT_DEFAULT).exists(_.toBoolean))
          .orElse(geomAttributes.headOption)
          .map(_.name)
    }
    val defaultDate = if (spec.options.get(IGNORE_INDEX_DTG).exists(toBoolean)) { None } else {
      val dateAttributes = spec.attributes.filter(_.clazz.isAssignableFrom(classOf[Date]))
      spec.options.get(DEFAULT_DATE_KEY).flatMap(dtg => dateAttributes.find(_.name == dtg))
          .orElse(dateAttributes.find(_.options.get(OPT_DEFAULT).exists(_.toBoolean)))
          .orElse(dateAttributes.headOption)
          .map(_.name)
    }

    val b = factory.map(new SimpleFeatureTypeBuilder(_)).getOrElse(new SimpleFeatureTypeBuilder())
    b.setNamespaceURI(namespace)
    b.setName(name)
    b.addAll(spec.attributes.map(_.toDescriptor).asJava)
    defaultGeom.foreach(b.setDefaultGeometry)

    val sft = b.buildFeatureType()
    sft.getUserData.putAll(spec.options.asJava)
    defaultDate.foreach(sft.setDtgField)
    sft
  }

  private def parseTypeName(name: String): (String, String) = {
    val nsIndex = name.lastIndexOf(':')
    val (namespace, local) = if (nsIndex == -1 || nsIndex == name.length - 1) {
      (null, name)
    } else {
      (name.substring(0, nsIndex), name.substring(nsIndex + 1))
    }
    (namespace, local)
  }

  @deprecated("Use AttributeIndex.indexed()")
  def getSecondaryIndexedAttributes(sft: SimpleFeatureType): Seq[AttributeDescriptor] = {
    sft.getIndices.flatMap { i =>
      i.attributes.headOption.map(sft.getDescriptor).filterNot(_.isInstanceOf[GeometryDescriptor])
    }
  }

  private [utils] def toBoolean(value: AnyRef): Boolean = value match {
    case null => false
    case bool: java.lang.Boolean => bool.booleanValue
    case bool: String => java.lang.Boolean.valueOf(bool).booleanValue
    case bool => java.lang.Boolean.valueOf(bool.toString).booleanValue
  }
}
