/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import java.util
import java.util.Collections

import com.beust.jcommander._
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools.status.GetSftConfigCommand.{Spec, TypeSafe}
import org.locationtech.geomesa.tools.{Command, DataStoreCommand, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

trait GetSftConfigCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  override val name: String = "get-sft-config"

  override def params: GetSftConfigParams

  override def execute(): Unit = {
    import scala.collection.JavaConversions._

    Command.user.info(s"Retrieving SFT for type name '${params.featureName}'")

    val sft = withDataStore(getSchema)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist in the provided datastore")
    }
    params.format.map(_.toLowerCase).foreach {
      case TypeSafe => Command.output.info(SimpleFeatureTypes.toConfigString(sft, !params.excludeUserData, params.concise))
      case Spec => Command.output.info(SimpleFeatureTypes.encodeType(sft, !params.excludeUserData))
      // shouldn't happen due to parameter validation
      case f => throw new ParameterException(s"Invalid format '$f'. Valid values are '$TypeSafe' and '$Spec'")
    }
  }

  def getSchema(ds: DS): SimpleFeatureType = ds.getSchema(params.featureName)

}

object GetSftConfigCommand {
  val Spec = "spec"
  val TypeSafe = "config"
}

// @Parameters(commandDescription = "Get the SimpleFeatureType of a feature")
trait GetSftConfigParams extends RequiredTypeNameParam {
  @Parameter(names = Array("--concise"), description = "Render in concise format", required = false)
  var concise: Boolean = false

  @Parameter(names = Array("--format"), description = "Output formats (allowed values are spec or config)", required = false, validateValueWith = classOf[FormatValidator])
  var format: java.util.List[String] = Collections.singletonList(Spec)

  @Parameter(names = Array("--exclude-user-data"), description = "Exclude user data", required = false)
  var excludeUserData: Boolean = false
}

class FormatValidator extends IValueValidator[java.util.List[String]] {
  override def validate(name: String, value: util.List[String]): Unit = {
    import scala.collection.JavaConversions._
    if (value == null || value.isEmpty || value.map(_.toLowerCase ).exists(v => v != Spec && v != TypeSafe)) {
      throw new ParameterException(s"Invalid value for format: ${Option(value).map(_.mkString(",")).orNull}")
    }
  }
}
