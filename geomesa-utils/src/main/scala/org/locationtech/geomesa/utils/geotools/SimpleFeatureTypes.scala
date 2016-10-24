/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.Date

import com.typesafe.config.Config
import org.apache.commons.lang.StringEscapeUtils
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.utils.geotools.AttributeSpec.GeomAttributeSpec
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType
import org.parboiled.errors.ParsingException

import scala.collection.JavaConversions._

object SimpleFeatureTypes {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

  object Configs {
    val TABLE_SHARING_KEY   = "geomesa.table.sharing"
    val DEFAULT_DATE_KEY    = "geomesa.index.dtg"
    val VIS_LEVEL_KEY       = "geomesa.visibility.level"
    val Z3_INTERVAL_KEY     = "geomesa.z3.interval"
    val XZ_PRECISION_KEY    = "geomesa.xz.precision"
    val TABLE_SPLITTER      = "table.splitter.class"
    val TABLE_SPLITTER_OPTS = "table.splitter.options"
    val MIXED_GEOMETRIES    = "geomesa.mixed.geometries"
    val RESERVED_WORDS      = "override.reserved.words" // note: doesn't start with geomesa so we don't persist it
    val KEYWORDS_KEY        = "geomesa.keywords"
    val ENABLED_INDICES     = "geomesa.indices.enabled"
    // keep around old values for back compatibility
    val ENABLED_INDEX_OPTS  = Seq(ENABLED_INDICES, "geomesa.indexes.enabled", "table.indexes.enabled")
    val ST_INDEX_SCHEMA_KEY = "geomesa.index.st.schema"
  }

  private [geomesa] object InternalConfigs {
    val GEOMESA_PREFIX      = "geomesa."
    val SCHEMA_VERSION_KEY  = "geomesa.version"
    val SHARING_PREFIX_KEY  = "geomesa.table.sharing.prefix"
    val USER_DATA_PREFIX    = "geomesa.user-data.prefix"
    val INDEX_VERSIONS      = "geomesa.indices"
    val KEYWORDS_DELIMITER  = "\u0000"
  }

  object AttributeOptions {
    val OPT_DEFAULT      = "default"
    val OPT_SRID         = "srid"
    val OPT_INDEX_VALUE  = "index-value"
    val OPT_INDEX        = "index"
    val OPT_STATS        = "keep-stats"
    val OPT_CARDINALITY  = "cardinality"
    val OPT_BIN_TRACK_ID = "bin-track-id"
    val OPT_CQ_INDEX     = "cq-index"
    val OPT_JSON         = "json"
  }

  private [geomesa] object AttributeConfigs {
    val USER_DATA_LIST_TYPE      = "subtype"
    val USER_DATA_MAP_KEY_TYPE   = "keyclass"
    val USER_DATA_MAP_VALUE_TYPE = "valueclass"
  }

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
      case e: ParsingException => throw new IllegalArgumentException(e)
    }
    createType(namespace, name, parsed)
  }


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
    createType(namespace, name, spec)
  }

  /**
    * Encode a SimpleFeatureType as a comma-separated String
    *
    * @param sft - SimpleFeatureType to encode
    * @param includeUserData - defaults to false
    * @return a string representing a serialization of the sft
    */
  def encodeType(sft: SimpleFeatureType, includeUserData: Boolean = false): String = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    lazy val userData = {
      val prefixes = sft.getUserDataPrefixes
      sft.getUserData.filter { case (k, v) => v != null && prefixes.exists(k.toString.startsWith) }
    }
    val suffix = if (!includeUserData || userData.isEmpty) { "" } else {
      userData.map { case (k, v) => s"$k='${StringEscapeUtils.escapeJava(v.toString)}'" }.mkString(";", ",", "")
    }

    sft.getAttributeDescriptors.map(encodeDescriptor(sft, _)).mkString("", ",", suffix)
  }

  def encodeDescriptor(sft: SimpleFeatureType, descriptor: AttributeDescriptor): String =
    AttributeSpec(sft, descriptor).toSpec

  def toConfig(sft: SimpleFeatureType, includeUserData: Boolean = true): Config =
    SimpleFeatureSpecConfig.toConfig(sft, includeUserData)

  def toConfigString(sft: SimpleFeatureType, includeUserData: Boolean = true, concise: Boolean = false): String =
    SimpleFeatureSpecConfig.toConfigString(sft, includeUserData, concise)

  /**
    * Renames a simple feature type. Preserves user data
    *
    * @param sft simple feature type
    * @param newName new name
    * @return
    */
  def renameSft(sft: SimpleFeatureType, newName: String) = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    builder.setName(newName)
    val renamed = builder.buildFeatureType()
    renamed.getUserData.putAll(sft.getUserData)
    renamed
  }

  private def createType(namespace: String, name: String, spec: SimpleFeatureSpec): SimpleFeatureType = {
    import AttributeOptions.OPT_DEFAULT
    import Configs.DEFAULT_DATE_KEY
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val defaultGeom = {
      val geomAttributes = spec.attributes.collect { case g: GeomAttributeSpec => g }
      geomAttributes.find(_.options.get(OPT_DEFAULT).exists(_.toBoolean))
          .orElse(geomAttributes.headOption)
          .map(_.name)
    }
    val defaultDate = {
      val dateAttributes = spec.attributes.filter(_.clazz.isAssignableFrom(classOf[Date]))
      spec.options.get(DEFAULT_DATE_KEY).flatMap(dtg => dateAttributes.find(_.name == dtg))
          .orElse(dateAttributes.find(_.options.get(OPT_DEFAULT).exists(_.toBoolean)))
          .orElse(dateAttributes.headOption)
          .map(_.name)
    }

    val b = new SimpleFeatureTypeBuilder()
    b.setNamespaceURI(namespace)
    b.setName(name)
    b.addAll(spec.attributes.map(_.toDescriptor))
    defaultGeom.foreach(b.setDefaultGeometry)

    val sft = b.buildFeatureType()
    sft.getUserData.putAll(spec.options)
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

  def getSecondaryIndexedAttributes(sft: SimpleFeatureType): Seq[AttributeDescriptor] =
    sft.getAttributeDescriptors.filter(ad => ad.isIndexed && !ad.isInstanceOf[GeometryDescriptor])
}
