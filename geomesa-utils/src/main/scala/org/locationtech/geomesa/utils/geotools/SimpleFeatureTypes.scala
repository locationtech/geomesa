/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.utils.geotools.NameableFeatureTypeFactory.NameableSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.{DefaultDtgField, IndexIgnoreDtg}
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.GeomAttributeSpec
import org.locationtech.geomesa.utils.geotools.sft._
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.`type`.{AttributeDescriptor, FeatureTypeFactory, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType
import org.parboiled.errors.ParsingException

object SimpleFeatureTypes {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  object Configs {

    // note: configs that don't start with 'geomesa' won't be persisted

    val DefaultDtgField       = "geomesa.index.dtg"
    val EnabledIndices        = "geomesa.indices.enabled"
    val FeatureExpiration     = "geomesa.feature.expiry"
    val FidsAreUuids          = "geomesa.fid.uuid"
    val FidsAreUuidEncoded    = "geomesa.fid.uuid-encoded"
    val IndexAttributeShards  = "geomesa.attr.splits"
    val IndexIdShards         = "geomesa.id.splits"
    val IndexIgnoreDtg        = "geomesa.ignore.dtg"
    val IndexVisibilityLevel  = "geomesa.visibility.level"
    val IndexXzPrecision      = "geomesa.xz.precision"
    val IndexZ3Interval       = "geomesa.z3.interval"
    val S3_INTERVAL_KEY       = "geomesa.s3.interval"
    val IndexZShards          = "geomesa.z.splits"
    val Keywords              = "geomesa.keywords"
    val MixedGeometries       = "geomesa.mixed.geometries"
    val OverrideDtgJoin       = "override.index.dtg.join"
    val OverrideReservedWords = "override.reserved.words"
    val QueryInterceptors     = "geomesa.query.interceptors"
    val StatsEnabled          = "geomesa.stats.enable"
    val TableCompressionType  = "geomesa.table.compression.type" // valid: gz(default), snappy, lzo, bzip2, lz4, zstd
    val TableLogicalTime      = "geomesa.logical.time"
    val TablePartitioning     = "geomesa.table.partition"
    val TableSharing          = "geomesa.table.sharing"
    val TableSplitterClass    = "table.splitter.class"
    val TableSplitterOpts     = "table.splitter.options"
    val TemporalPriority      = "geomesa.temporal.priority"
    val UpdateBackupMetadata  = "schema.update.backup.metadata"
    val UpdateRenameTables    = "schema.update.rename.tables"

    // keep around old values for back compatibility
    @deprecated("EnabledIndices")
    val ENABLED_INDEX_OPTS: Seq[String] = Seq(EnabledIndices, "geomesa.indexes.enabled", "table.indexes.enabled")
  }

  private [geomesa] object InternalConfigs {
    val GeomesaPrefix          = "geomesa."
    val TableSharingPrefix     = "geomesa.table.sharing.prefix"
    val UserDataPrefix         = "geomesa.user-data.prefix"
    val IndexVersions          = "geomesa.indices"
    val PartitionSplitterClass = "geomesa.splitter.class"
    val PartitionSplitterOpts  = "geomesa.splitter.opts"
    val RemoteVersion          = "gm.remote.version" // note: doesn't start with geomesa so we don't persist it
    val KeywordsDelimiter      = "\u0000"
  }

  object AttributeOptions {
    val OptCardinality  = "cardinality"
    val OptColumnGroups = "column-groups"
    val OptCqIndex      = "cq-index"
    val OptDefault      = "default"
    val OptIndex        = "index"
    val OptIndexValue   = "index-value"
    val OptJson         = "json"
    val OptPrecision    = "precision"
    val OptSrid         = "srid"
    val OptStats        = "keep-stats"
  }

  private [geomesa] object AttributeConfigs {
    val UserDataListType     = "subtype"
    val UserDataMapKeyType   = "keyclass"
    val UserDataMapValueType = "valueclass"
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
    sft.getUserData.asScala.foreach { case (k: AnyRef, v: AnyRef) =>
      if (v != null && prefixes.exists(k.toString.startsWith)) {
        result.append(encodeUserData(k, v)).append(",")
      }
    }
    if (result.lengthCompare(1) > 0) { result.substring(0, result.length - 1) } else { "" }
  }

  def encodeUserData(data: java.util.Map[_ <: AnyRef, _ <: AnyRef]): String = {
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
    * Serializes a feature type to a single string
    *
    * @param sft feature type
    * @return
    */
  def serialize(sft: SimpleFeatureType): String =
    StringSerialization.encodeSeq(Seq(sft.getTypeName, encodeType(sft, includeUserData = true)))

  /**
    * Deserializes a serialized feature type string
    *
    * @param sft serialized feature type
    * @return
    */
  def deserialize(sft: String): SimpleFeatureType = {
    val Seq(name, spec) = StringSerialization.decodeSeq(sft)
    SimpleFeatureTypes.createType(name, spec)
  }

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
  def immutable(
      sft: SimpleFeatureType,
      extraData: java.util.Map[_ <: AnyRef, _ <: AnyRef] = null): SimpleFeatureType = {
    sft match {
      case immutable: ImmutableSimpleFeatureType if extraData == null || extraData.isEmpty => immutable
      case _ =>
        val schema = new java.util.ArrayList[AttributeDescriptor](sft.getAttributeCount)
        var geom: GeometryDescriptor = null
        var i = 0
        while (i < sft.getAttributeCount) {
          val descriptor = sft.getDescriptor(i)
          val im = immutable(descriptor)
          if (descriptor == sft.getGeometryDescriptor) {
            geom = im.asInstanceOf[GeometryDescriptor]
          }
          schema.add(im)
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

  def immutable(descriptor: AttributeDescriptor): AttributeDescriptor = {
    descriptor match {
      case d: ImmutableAttributeDescriptor => d
      case d: ImmutableGeometryDescriptor => d
      case d: GeometryDescriptor =>
        new ImmutableGeometryDescriptor(
          d.getType, d.getName, d.getMinOccurs, d.getMaxOccurs, d.isNillable, d.getDefaultValue, d.getUserData)
      case d =>
        new ImmutableAttributeDescriptor(
          d.getType, d.getName, d.getMinOccurs, d.getMaxOccurs, d.isNillable, d.getDefaultValue, d.getUserData)
      case null => throw new NullPointerException("Descriptor is null")
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

  /**
   * Rename an attribute descriptor
   *
   * @param descriptor descriptor
   * @param newName new name
   * @return
   */
  def renameDescriptor(descriptor: AttributeDescriptor, newName: String): AttributeDescriptor = {
    val builder = new AttributeTypeBuilder()
    builder.init(descriptor)
    builder.buildDescriptor(newName)
  }

  /**
    * Compare two feature types. This method compares the the schemas of each feature type, i.e. the
    * names and type bindings of the attributes. The result will be:
    *
    * <ul>
    *   <li>a positive int if type A is a sub type or reorder of type B</li>
    *   <li>zero if type A has the same schema as type B</li>
    *   <li>a negative int if none of the above</li>
    * </ul>
    *
    * Note that this shouldn't be used for sorting.
    *
    * Note: GeoTools DataUtilities has a similar method that does not work correctly
    *
    * @param a schema A
    * @param b schema B
    * @return comparison
    */
  def compare(a: SimpleFeatureType, b: SimpleFeatureType): Int = {
    if (a.getAttributeCount > b.getAttributeCount) {
      return -1 // not equals, subtype or reorder
    }

    var exact = a.getAttributeCount == b.getAttributeCount

    var i = 0
    while (i < a.getAttributeCount) {
      val ad = a.getDescriptor(i)
      val bd = b.getDescriptor(i)
      if (ad.getLocalName == bd.getLocalName) {
        if (ad.getType.getBinding != bd.getType.getBinding) {
          if (bd.getType.getBinding.isAssignableFrom(ad.getType.getBinding)) {
            exact = false // we've seen a subtyping
          } else {
            return -1 // not a subtype/reorder
          }
        }
      } else {
        val reorder = b.getDescriptor(ad.getLocalName)
        if (reorder == null || !reorder.getType.getBinding.isAssignableFrom(ad.getType.getBinding)) {
          return -1 // not a subtype/reorder
        } else {
          exact = false // we've seen a reordering
        }
      }
      i += 1
    }

    if (exact) { 0 } else {
      1  // we haven't hit an absolute inequality, but we've found a reordering or subtype
    }
  }

  private def createFeatureType(namespace: String,
                                name: String,
                                spec: SimpleFeatureSpec,
                                factory: Option[FeatureTypeFactory] = None): SimpleFeatureType = {
    import AttributeOptions.OptDefault

    val defaultGeom = {
      val geomAttributes = spec.attributes.collect { case g: GeomAttributeSpec => g }
      geomAttributes.find(_.options.get(OptDefault).exists(_.toBoolean))
          .orElse(geomAttributes.headOption)
          .map(_.name)
    }
    val defaultDate = if (spec.options.get(IndexIgnoreDtg).exists(toBoolean)) { None } else {
      val dateAttributes = spec.attributes.filter(a => classOf[Date].isAssignableFrom(a.clazz))
      spec.options.get(DefaultDtgField).flatMap(dtg => dateAttributes.find(_.name == dtg))
          .orElse(dateAttributes.find(_.options.get(OptDefault).exists(_.toBoolean)))
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

  private [utils] def toBoolean(value: AnyRef): Boolean = value match {
    case null => false
    case bool: java.lang.Boolean => bool.booleanValue
    case bool: String => java.lang.Boolean.valueOf(bool).booleanValue
    case bool => java.lang.Boolean.valueOf(bool.toString).booleanValue
  }
}
