/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.util
import java.util.Locale
import java.util.regex.Pattern

import com.beust.jcommander.converters.BaseConverter
import com.beust.jcommander.{Parameter, ParameterException}
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.tools.DistributedRunParam.ModeConverter
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.utils.ParameterConverters.{ErrorModeConverter, FilterConverter, HintConverter}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.filter.Filter

/**
  * Shared parameters as individual traits
  */

trait QueryParams extends CatalogParam with RequiredTypeNameParam with CqlFilterParam with OptionalAttributesParam

trait CatalogParam {
  @Parameter(names = Array("-c", "--catalog"), description = "Catalog table for GeoMesa datastore", required = true)
  var catalog: String = _
}

trait TypeNameParam {
  def featureName: String
}

trait RequiredTypeNameParam extends TypeNameParam {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate", required = true)
  var featureName: String = _
}

trait OptionalTypeNameParam extends TypeNameParam {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate")
  var featureName: String = _
}

trait PasswordParams {
  @Parameter(names = Array("-p", "--password"), description = "Connection password")
  var password: String = _
}

trait KerberosParams {
  @Parameter(names = Array("--keytab"), description = "Path to Kerberos keytab file")
  var keytab: String = _
}

trait RequiredCredentialsParams extends PasswordParams {
  @Parameter(names = Array("-u", "--user"), description = "Connection user name", required = true)
  var user: String = _
}

trait OptionalCredentialsParams extends PasswordParams {
  @Parameter(names = Array("-u", "--user"), description = "Connection user name")
  var user: String = _
}

trait FeatureSpecParam {
  def spec: String
}

trait RequiredFeatureSpecParam extends FeatureSpecParam {
  @Parameter(names = Array("-s", "--spec"),
    description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either", required = true)
  var spec: String = _
}

trait OptionalFeatureSpecParam extends FeatureSpecParam {
  @Parameter(names = Array("-s", "--spec"), description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either")
  var spec: String = _
}

trait CqlFilterParam {
  def cqlFilter: Filter
}

trait RequiredCqlFilterParam extends CqlFilterParam {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate", required = true, converter = classOf[FilterConverter])
  var cqlFilter: Filter = _
}

trait OptionalCqlFilterParam extends CqlFilterParam {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate", converter = classOf[FilterConverter])
  var cqlFilter: Filter = _
}

trait QueryHintsParams {
  @Parameter(names = Array("--hints"), description = "Query hints to set, in the form key1=value1;key2=value2", required = false, converter = classOf[HintConverter])
  var hints: java.util.Map[String, String] = _
}

trait OptionalDtgParam {
  @Parameter(names = Array("--dtg"), description = "DateTime field name to use as the default dtg")
  var dtgField: String = _
}

trait AttributesParam {
  def attributes: java.util.List[String]
}

trait OptionalAttributesParam extends AttributesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (comma-separated)")
  var attributes: java.util.List[String] = _
}

trait RequiredAttributesParam extends AttributesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (comma-separated)", required = true)
  var attributes: java.util.List[String] = _
}

trait OptionalForceParam {
  @Parameter(names = Array("--force"), description = "Force execution without prompt")
  var force: Boolean = false
}

trait OptionalPatternParam {
  @Parameter(names = Array("--pattern"), description = "Regular expression for simple feature type names")
  var pattern: Pattern = _
}

trait OptionalZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = _
}

trait InputFilesParam {
  @Parameter(description = "<file>...")
  var files: java.util.List[String] = new util.ArrayList[String]()
}

trait OptionalInputFormatParam extends InputFilesParam {
  @Parameter(names = Array("--input-format"), description = "File format of input files (shp, csv, tsv, avro, etc). Optional, auto-detection will be attempted")
  var format: String = _
}

trait ConverterConfigParam {
  @Parameter(names = Array("-C", "--converter"), description = "GeoMesa converter specification as a config string, file name, or name of an available converter")
  var config: String = _

  @Parameter(names = Array("--converter-error-mode"), description = "Override the converter error mode - 'skip-bad-records' or 'raise-errors'", converter = classOf[ErrorModeConverter])
  var errorMode: ErrorMode = _
}

object IndexParam {

  @throws[ParameterException]
  def loadIndex[DS <: GeoMesaDataStore[DS]](ds: DS,
                                            typeName: String,
                                            index: String,
                                            mode: IndexMode): GeoMesaFeatureIndex[_, _] = {
    val sft = ds.getSchema(typeName)
    val indices = ds.manager.indices(sft, mode)

    lazy val available = indices.flatMap(i => Seq(i.name, i.identifier)).distinct.mkString(", ")

    def single(indices: Seq[GeoMesaFeatureIndex[_, _]]): Option[GeoMesaFeatureIndex[_, _]] = indices match {
      case Nil => None
      case Seq(i) => Some(i)
      case _ => throw new ParameterException(s"Specified index '$index' is ambiguous. Available indices are: $available")
    }

    def byId: Option[GeoMesaFeatureIndex[_, _]] = indices.find(_.identifier.equalsIgnoreCase(index))
    def byName: Option[GeoMesaFeatureIndex[_, _]] = single(indices.filter(_.name.equalsIgnoreCase(index)))
    // check for attr vs join index name
    def byJoin: Option[GeoMesaFeatureIndex[_, _]] = if (!index.equalsIgnoreCase(AttributeIndex.name)) { None } else {
      single(indices.filter(_.name == AttributeIndex.JoinIndexName))
    }

    byId.orElse(byName).orElse(byJoin).getOrElse {
      throw new ParameterException(s"Specified index '$index' not found. Available indices are: $available")
    }
  }
}

trait IndexParam {
  def index: String
}

trait OptionalIndexParam extends IndexParam {

  @Parameter(names = Array("--index"), description = "Specify a particular GeoMesa index", required = false)
  var index: String = _

  @throws[ParameterException]
  def loadIndex[DS <: GeoMesaDataStore[DS]](ds: DS, typeName: String, mode: IndexMode): Option[GeoMesaFeatureIndex[_, _]] =
    Option(index).filterNot(_.isEmpty).map(IndexParam.loadIndex(ds, typeName, _, mode))
}

trait RequiredIndexParam extends IndexParam {

  @Parameter(names = Array("--index"), description = "Specify a particular GeoMesa index", required = true)
  var index: String = _

  @throws[ParameterException]
  def loadIndex[DS <: GeoMesaDataStore[DS]](ds: DS, typeName: String, mode: IndexMode): GeoMesaFeatureIndex[_, _] =
    IndexParam.loadIndex(ds, typeName, index, mode)
}

trait IndicesParam {

  import scala.collection.JavaConverters._

  @Parameter(names = Array("--index"), description = "Specify GeoMesa index(es) - comma-separate or use multiple flags", required = true)
  var indexNames: java.util.List[String] = _

  @throws[ParameterException]
  def loadIndices[DS <: GeoMesaDataStore[DS]](ds: DS, typeName: String, mode: IndexMode): Seq[GeoMesaFeatureIndex[_, _]] =
    indexNames.asScala.map(IndexParam.loadIndex(ds, typeName, _, mode))
}

trait DistributedRunParam {
  @Parameter(names = Array("--run-mode"), description = "Run locally or on a cluster", required = false, converter = classOf[ModeConverter])
  var mode: RunMode = _
}

object DistributedRunParam {
  object RunModes extends Enumeration {
    type RunMode = Value
    val Distributed, DistributedCombine, Local = Value
  }

  class ModeConverter(name: String) extends BaseConverter[RunMode](name) {
    override def convert(value: String): RunMode = {
      Option(value).flatMap(v => RunModes.values.find(_.toString.equalsIgnoreCase(v))).getOrElse {
        val error = s"run-mode. Valid values are: ${RunModes.values.map(_.toString.toLowerCase(Locale.US)).mkString(", ")}"
        throw new ParameterException(getErrorString(value, error))
      }
    }
  }
}

trait OutputPathParam {
  @Parameter(names = Array("--output"), description = "Path to use for writing output", required = true)
  var outputPath: String = _
}
