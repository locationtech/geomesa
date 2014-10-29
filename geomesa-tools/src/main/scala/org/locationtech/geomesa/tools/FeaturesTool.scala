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

package org.locationtech.geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloFeatureReader}
import org.locationtech.geomesa.core.index.SF_PROPERTY_START_TIME

import scala.collection.JavaConversions._
import scala.util.Try

class FeaturesTool(config: FeatureArguments, password: String) extends Logging with AccumuloProperties {

  val instance = config.instanceName.getOrElse(instanceName)
  val zookeepersString = config.zookeepers.getOrElse(zookeepers)

  val ds: AccumuloDataStore = Try({
    DataStoreFinder.getDataStore(Map(
      "instanceId"   -> instance,
      "zookeepers"   -> zookeepersString,
      "user"         -> config.username,
      "password"     -> password,
      "tableName"    -> config.catalog,
      "visibilities" -> config.visibilities.orNull,
      "auths"        -> config.auths.orNull)).asInstanceOf[AccumuloDataStore]
  }).getOrElse{
    logger.error("Cannot connect to Accumulo. Please check your configuration and try again.")
    sys.exit()
  }

  def listFeatures() {
    if (!config.toStdOut) { logger.info(s"Listing features on '${config.catalog}'. Just a few moments...") }
    val featureCount = if (ds.getTypeNames.size == 1) {
      s"1 feature exists on '${config.catalog}'. It is: "
    } else if (ds.getTypeNames.size == 0) {
      s"0 features exist on '${config.catalog}'. This catalog table might not yet exist."
    } else {
      s"${ds.getTypeNames.size} features exist on '${config.catalog}'. They are: "
    }
    if (!config.toStdOut) { logger.info(s"$featureCount") }
    ds.getTypeNames.foreach(name =>
      logger.info(s"$name")
    )
  }

  def describeFeature() {
    if (!config.toStdOut) { logger.info(s"Describing attributes of feature '${config.featureName}' on " +
      s"catalog table '${config.catalog}'. Just a few moments...") }
    try {
      val sft = ds.getSchema(config.featureName)
      sft.getAttributeDescriptors.foreach(attr => {
        val defaultValue = attr.getDefaultValue
        val attrType = attr.getType.getBinding.getSimpleName
        var attrString = s"${attr.getLocalName}"
        if (!config.toStdOut) {
          attrString = attrString.concat(s": $attrType ")
          if (sft.getUserData.getOrElse(SF_PROPERTY_START_TIME, "") == attr.getLocalName) {
            attrString = attrString.concat("(Time-index) ")
          } else if (sft.getGeometryDescriptor == attr) {
            attrString = attrString.concat("(Geo-index) ")
          } else if (attr.getUserData.getOrElse("index", false).asInstanceOf[java.lang.Boolean]) {
            attrString = attrString.concat("(Indexed) ")
          }
          if (defaultValue != null) {
            attrString = attrString.concat(s"- Default Value: $defaultValue")
          }
        }
        logger.info(s"$attrString")
      })
    } catch {
      case npe: NullPointerException => logger.error("Error: feature not found. Please ensure " +
        "that all arguments are correct in the previous command.")
      case e: Exception => logger.error("Error describing feature")
    }
  }

  def createFeature() {
    FeatureCreator.createFeature(
      ds, config.spec, config.featureName, config.dtField, config.sharedTable, config.catalog, config.maxShards
    )
    sys.exit()
  }

  def deleteFeature() {
    val confirmation = if (config.forceDelete) {
      true
    } else {
      print(s"Delete '${config.featureName}' on catalog table '${config.catalog}'? (yes/no) : ")
      if (System.console().readLine() == "yes") { true } else { false }
    }
    if (confirmation) {
      logger.info(s"Deleting '${config.catalog}_${config.featureName}'. This will take longer " +
        "than other commands to complete. Just a few moments...")
      try {
        ds.removeSchema(config.featureName)
        if (!ds.getNames.contains(config.featureName)) {
          logger.info(s"Feature '${config.catalog}_${config.featureName}' successfully deleted.")
        } else {
          logger.error(s"There was an error deleting feature '${config.catalog}_${config.featureName}'" +
            "Please check that all arguments are correct in the previous command.")
        }
      } catch {
        case re: RuntimeException => false
        case e: Exception => false
      }
    }
  }

  def explainQuery() {
    val q = new Query()
    val t = Transaction.AUTO_COMMIT
    q.setTypeName(config.featureName)

    val f = ECQL.toFilter(config.query)
    q.setFilter(f)

    try {
      val afr = ds.getFeatureReader(q, t).asInstanceOf[AccumuloFeatureReader]
      afr.explainQuery(q)
    } catch {
      case re: RuntimeException => logger.error(s"Error: Could not explain the query. Please " +
        s"ensure that all arguments are correct in the previous command.")
      case e: Exception => logger.error(s"Error: Could not explain the query.")
    }
  }
}