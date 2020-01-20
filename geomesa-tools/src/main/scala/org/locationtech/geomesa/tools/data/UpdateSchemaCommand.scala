/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.data

import java.io.IOException
import java.util.Collections

import com.beust.jcommander.{Parameter, ParameterException}
import org.geotools.data.DataStore
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand.UpdateSchemaParams
import org.locationtech.geomesa.tools.utils.{NoopParameterSplitter, Prompt}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser

import scala.util.control.NonFatal

/**
  * Invoke `updateSchema` on a datastore
  *
  * @tparam DS data store type
  */
trait UpdateSchemaCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  override val name = "update-schema"
  override def params: UpdateSchemaParams

  override def execute(): Unit = withDataStore(update)

  protected def update(ds: DS): Unit = {
    // ensure we have an operation
    if (params.rename == null &&
        Seq(params.renameAttributes, params.attributes, params.plusKeywords, params.minusKeywords).forall(_.isEmpty)) {
      throw new ParameterException("Please specify an update operation")
    }

    val sft = try { ds.getSchema(params.featureName) } catch { case _: IOException => null }
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist in the data store")
    }

    var n = 0
    // numbering for our prompts
    def number: Int = { n += 1; n }
    val prompts = new StringBuilder()
    var canRenameTables = false

    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)

    Option(params.rename).foreach { rename =>
      rename.indexOf(':') match {
        case -1 => builder.setName(rename)
        case i  => builder.setNamespaceURI(rename.substring(0, i)); builder.setName(rename.substring(i + 1))
      }
      prompts.append(s"\n  $number: Renaming schema to '$rename'")
      canRenameTables = true
    }

    params.renameAttributes.asScala.grouped(2).foreach { case Seq(from, to) =>
      val i = sft.indexOf(from)
      if (i == -1) {
        throw new ParameterException(s"Attribute '$from' does not exist in the schema")
      }
      val attribute = new AttributeTypeBuilder()
      attribute.init(sft.getDescriptor(i))
      builder.set(i, attribute.buildDescriptor(to))
      if (sft.getGeomField == from) {
        builder.setDefaultGeometry(to)
      }
      prompts.append(s"\n  $number: Renaming attribute '$from' to '$to'")
      canRenameTables = canRenameTables || sft.getIndices.exists(_.attributes.contains(from))
    }

    if (canRenameTables && params.renameTables) {
      prompts.append(s"\n  $number: Renaming index tables (WARNING may be expensive)")
    }

    params.attributes.asScala.foreach { attribute =>
      val spec = try { SimpleFeatureSpecParser.parseAttribute(attribute) } catch {
        case NonFatal(e) => throw new ParameterException(s"Invalid attribute spec: $attribute", e)
      }
      builder.add(spec.toDescriptor)
      prompts.append(s"\n  $number: Adding attribute '${spec.name}' of type ${spec.clazz.getName}")
    }

    val updated = builder.buildFeatureType()
    updated.getUserData.putAll(sft.getUserData)

    if (!params.plusKeywords.isEmpty) {
      val keywords = params.plusKeywords.asScala
      updated.addKeywords(keywords.toSet)
      prompts.append(s"\n  $number: Adding keywords: '${keywords.mkString("', '")}'")
    }
    if (!params.minusKeywords.isEmpty) {
      val keywords = params.minusKeywords.asScala
      updated.removeKeywords(keywords.toSet)
      prompts.append(s"\n  $number: Removing keywords: '${keywords.mkString("', '")}'")
    }

    Option(params.enableStats).map(_.booleanValue()).foreach { enable =>
      sft.setStatsEnabled(enable)
      prompts.append(s"\n  $number: ${if (enable) { "En" } else { "Dis" }}abling stats")
    }

    if (params.renameTables) {
      updated.getUserData.put(SimpleFeatureTypes.Configs.UpdateRenameTables, java.lang.Boolean.TRUE)
    }
    if (params.noBackup) {
      updated.getUserData.put(SimpleFeatureTypes.Configs.UpdateBackupMetadata, java.lang.Boolean.FALSE)
    }

    Command.user.info(s"Preparing to update schema '${sft.getTypeName}':$prompts")
    if (params.force || Prompt.confirm("Continue (y/n)? ")) {
      Command.user.info("Updating, please wait...")
      ds.updateSchema(sft.getTypeName, updated)
      Command.user.info("Update complete")
    }
  }
}

object UpdateSchemaCommand {

  // @Parameters(commandDescription = "Update a GeoMesa feature type")
  trait UpdateSchemaParams extends RequiredTypeNameParam with OptionalForceParam {

    @Parameter(names = Array("--rename"), description = "Update the feature type name")
    var rename: String = _

    @Parameter(
      names = Array("--rename-attribute"),
      description = "Rename an existing attribute, by specifying the current name and the new name",
      splitter = classOf[NoopParameterSplitter],
      arity = 2)
    var renameAttributes: java.util.List[String] = Collections.emptyList()

    @Parameter(
      names = Array("--add-attribute"),
      description = "Add a new attribute, specified as a GeoTools spec string (e.g. 'dtg:Date:index=true')",
      splitter = classOf[NoopParameterSplitter])
    var attributes: java.util.List[String] = Collections.emptyList()

    @Parameter(
      names = Array("--add-keyword"),
      description = "Add a new keyword to the feature type user data",
      splitter = classOf[NoopParameterSplitter])
    var plusKeywords: java.util.List[String] = Collections.emptyList()

    @Parameter(
      names = Array("--remove-keyword"),
      description = "Remove a keyword from the feature type user data",
      splitter = classOf[NoopParameterSplitter])
    var minusKeywords: java.util.List[String] = Collections.emptyList()

    @Parameter(
      names = Array("--enable-stats"),
      description = "Enable or disable stats for the feature type",
      arity = 1)
    var enableStats: java.lang.Boolean = _

    @Parameter(
      names = Array("--rename-tables"),
      description = "When updating the type name, also rename index tables to match the new type name")
    var renameTables: Boolean = false

    @Parameter(
      names = Array("--no-backup"),
      description = "Don't back up data store metadata before updating the schema")
    var noBackup: Boolean = false
  }
}
