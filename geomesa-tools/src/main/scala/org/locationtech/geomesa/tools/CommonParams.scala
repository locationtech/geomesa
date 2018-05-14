/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.DistributedRunParam.ModeConverter
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.ParameterConverters.{FilterConverter, HintConverter}
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

trait OptionalSharedTablesParam {
  @Parameter(names = Array("--use-shared-tables"), description = "Use shared tables for feature storage (true/false)", arity = 1)
  var useSharedTables: Boolean = true //default to true in line with datastore
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

trait InputFormatParam extends InputFilesParam {

  def format: String

  def fmt: DataFormats.DataFormat = {
    import scala.collection.JavaConversions._
    val fmtParam = Option(format).flatMap(f => DataFormats.values.find(_.toString.equalsIgnoreCase(f)))
    lazy val fmtFile = files.flatMap(DataFormats.fromFileName(_).right.toOption).headOption
    fmtParam.orElse(fmtFile).orNull
  }
}

trait OptionalInputFormatParam extends InputFormatParam {
  @Parameter(names = Array("--input-format"), description = "File format of input files (shp, csv, tsv, avro, etc). Optional, autodetection will be attempted.")
  var format: String = _
}

trait ConverterConfigParam {
  def config: String
}

trait OptionalConverterConfigParam extends ConverterConfigParam {
  @Parameter(names = Array("-C", "--converter"), description = "GeoMesa converter specification as a config string, file name, or name of an available converter",
    required = false)
  var config: String = _
}

trait RequiredConverterConfigParam extends ConverterConfigParam {
  @Parameter(names = Array("-C", "--converter"), description = "GeoMesa converter specification as a config string, file name, or name of an available converter",
    required = true)
  var config: String = _
}

trait IndexParam extends TypeNameParam {

  def index: String

  @throws[ParameterException]
  def loadRequiredIndex(ds: GeoMesaDataStore[_, _, _], mode: IndexMode): GeoMesaFeatureIndex[_, _, _] =
    loadIndex(ds, mode).get

  @throws[ParameterException]
  def loadIndex(ds: GeoMesaDataStore[_, _, _], mode: IndexMode): Option[GeoMesaFeatureIndex[_, _, _]] = {
    Option(index).filter(_.length > 0).map { name =>
      val sft = ds.getSchema(featureName)
      def all = ds.manager.indices(sft, None, mode).asInstanceOf[Seq[GeoMesaFeatureIndex[_, _, _]]]
      ds.manager.indices(sft, Some(name), mode) match {
        case Nil =>
          throw new ParameterException(s"Specified index '$index' not found. Available indices are: " +
              all.map(i => s"${i.name}, ${i.identifier}").mkString(", "))
        case Seq(idx) =>
          idx.asInstanceOf[GeoMesaFeatureIndex[_ <: GeoMesaDataStore[_, _, _], _ <: WrappedFeature, _]]
        case s =>
          throw new ParameterException(s"Specified index '$index' is ambiguous. Available indices are: " +
              all.map(_.identifier).mkString(", "))
      }
    }
  }
}

trait OptionalIndexParam extends IndexParam {
  @Parameter(names = Array("--index"), description = "Specify a particular index to query", required = false)
  var index: String = _
}

trait RequiredIndexParam extends IndexParam {
  @Parameter(names = Array("--index"), description = "Specify a particular GeoMesa index", required = true)
  var index: String = _
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
