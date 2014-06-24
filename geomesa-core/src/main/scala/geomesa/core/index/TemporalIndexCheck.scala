package geomesa.core.index

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.index
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import scala.collection.JavaConverters._

/**
 * Utility class for emitting a warning to the user if a SimpleFeatureType contains a temporal attribute, but
 * none is used in the index.
 *
 * Furthermore, this class presents a candidate to be used in this case.
 *
 * This is useful since the only symptom of this mistake is slower than normal queries on temporal ranges.
 */

case class TemporalIndexCheck(sft:SimpleFeatureType) extends Logging {
  // check if the attribute is actually present
  val hasValidDtgField = index.getDtgDescriptor(sft).isDefined
  // get all attributes which may be used
  val dtgCandidates = scanForTemporalAttributes(sft)
  // we may wish to use the first acceptable attribute found, although we currently require just one match
  val firstDtgCandidate = dtgCandidates.headOption
  val hasValidDtgCandidate = dtgCandidates.nonEmpty
  // if there is just one valid dtg candidate, then we can safely use it
  val dtgShouldBeSet =  !hasValidDtgField  && dtgCandidates.length == 1
  // emit a warning to the user
  if (!hasValidDtgField && hasValidDtgCandidate) emitDtgWarning(dtgCandidates)
  // if we are going to mutate UserData, notify the user
  if (dtgShouldBeSet) firstDtgCandidate.map { text =>emitDtgNotification(text) }

  def emitDtgWarning(matches: List[String]) {
    lazy val theWarning =
                s"""
                    |__________Possible problem detected in the SimpleFeatureType_____________
                    |SF_PROPERTY_START_TIME points to no existing SimpleFeature attribute, or isn't defined.
                    |However, the following attribute(s) could be used in GeoMesa's temporal index:
                    |${matches.mkString("\n","\n","\n")}
                    |Please note that while queries on a temporal attribute will still work,
                    |queries will be faster if SF_PROPERTY_START_TIME, located in the SimpleFeatureType's UserData,
                    |points to the attribute's name
                """.stripMargin
        logger.warn(theWarning)
  }

  def emitDtgNotification(temporalAttributeName: String) {
    lazy val theNotification =
      """
        | There is just one temporal attribute detected in the SimpleFeatureType.
        | SF_PROPERTY_START_TIME will be set to point to:
        | ${temporalAttributeName}
      """.stripMargin
    logger.warn(theNotification)
  }

  def scanForTemporalAttributes(sft: SimpleFeatureType): List[String] =
    for {
      descriptor: AttributeDescriptor <- sft.getAttributeDescriptors.asScala.toList
        theType = descriptor.getType.getBinding.getCanonicalName
        if theType == "java.util.Date"
          temporalCandidateName = descriptor.getLocalName
    } yield temporalCandidateName
}
