package geomesa.core.index

import collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.index
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Utility object for emitting a warning to the user if a SimpleFeatureType contains a temporal attribute, but
 * none is used in the index.
 *
 * This is useful since the only symptom of this mistake is slower than normal queries on temporal ranges.
 */

object TemporalIndexCheck extends Logging {

  def checkForValidDtgField(sft: SimpleFeatureType): Unit = {
    // the dtgDescriptor will be None if SF_PROPERTY_START_TIME doesn't point to a valid attribute
    val dtgDescriptor = index.getDtgDescriptor(sft)
    dtgDescriptor getOrElse {
      val matches = scanForTemporalAttribute(sft)
      if (matches.nonEmpty) {
        val theWarning = "\n" +
          "__________Possible problem detected in the SimpleFeatureType_____________ \n " +
          "SF_PROPERTY_START_TIME points to no existing SimpleFeature attribute, or isn't defined. " +
          "However, the following attribute(s) could be used in GeoMesa's temporal index: \n" +
          matches.mkString("\n", "\n", "\n") + "\n" +
          "Please note that while queries on a temporal attribute will still work, " +
          "queries will be faster if SF_PROPERTY_START_TIME, located in the SimpleFeatureType's UserData, " +
          "points to the attribute's name" + "\n"
        logger.warn(theWarning)
      }
    }

  }

  def scanForTemporalAttribute(sft: SimpleFeatureType): List[String] = {
    for {
      descriptor: AttributeDescriptor <- sft.getAttributeDescriptors.asScala.toList
        theName = descriptor.getLocalName
        theType = descriptor.getType.getBinding.getCanonicalName
        if theType == "java.util.Date"
          temporalCandidate = theName.toString
    } yield temporalCandidate
  }
}
