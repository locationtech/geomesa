package org.locationtech.geomesa.tools.common.commands

import com.typesafe.scalalogging.LazyLogging

import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
import org.locationtech.geomesa.utils.stats.IndexCoverage

import scala.collection.JavaConversions._
import scala.util.control.NonFatal


trait DescribeCommand extends CommandWithDataStore with LazyLogging {
  val command = "describe"
  val params: FeatureTypeNameParam
  def execute() = {


    logger.info(s"Describing attributes of feature '${params.featureName}'")
    try {
      val sft = ds.getSchema(params.featureName)

      val sb = new StringBuilder()
      sft.getAttributeDescriptors.foreach { attr =>
        sb.clear()
        val name = attr.getLocalName
        val indexCoverage = attr.getIndexCoverage()

        // TypeName
        sb.append(name)
        sb.append(": ")
        sb.append(attr.getType.getBinding.getSimpleName)

        if (sft.getDtgField.exists(_ == name))        sb.append(" (ST-Time-index)")
        if (sft.getGeometryDescriptor == attr)        sb.append(" (ST-Geo-index)")
        if (indexCoverage == IndexCoverage.JOIN)      sb.append(" (Indexed - Join)")
        else if (indexCoverage == IndexCoverage.FULL) sb.append(" (Indexed - Full)")
        if (attr.getDefaultValue != null)             sb.append("- Default Value: ", attr.getDefaultValue)

        println(sb.toString())
      }

      val userData = sft.getUserData
      if (!userData.isEmpty) {
        println("\nUser data:")
        userData.foreach {
          case (KEYWORDS_KEY, v) => println(s"  $KEYWORDS_KEY: " +
            "[".concat(v.asInstanceOf[String].split(KEYWORDS_DELIMITER)
              .map{ "\"%s\"".format(_)}.mkString(",").concat("]")))
          case (key, value) => println(s"  $key: $value")
        }
      }

    } catch {
      case npe: NullPointerException =>
        logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        logger.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
      case NonFatal(e) =>
        logger.warn(s"Non fatal error encountered describing feature '${params.featureName}': ", e)
    } finally {
      ds.dispose()
    }
  }

}