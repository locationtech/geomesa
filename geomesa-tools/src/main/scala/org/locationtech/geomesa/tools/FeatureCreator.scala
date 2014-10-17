package org.locationtech.geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

object FeatureCreator extends Logging {

  def createFeature(ds: AccumuloDataStore, sftspec: String, featureName: String, dtField: Option[String], sharedTable: Option[Boolean], catalog: String, maxShards: Option[Int] = None): Unit = {
    logger.info(s"Creating '$featureName' on catalog table '$catalog' with spec " +
      s"'$sftspec'. Just a few moments...")

    if (ds.getSchema(featureName) == null) {

      logger.info("\tCreating GeoMesa tables...")

      val sft = SimpleFeatureTypes.createType(featureName, sftspec)
      if (dtField.orNull != null) {
        // Todo: fix logic here, it is a bit strange
        sft.getUserData.put(SF_PROPERTY_START_TIME, dtField.getOrElse(Constants.SF_PROPERTY_START_TIME))
      }

      sharedTable.foreach { org.locationtech.geomesa.core.index.setTableSharing(sft, _) }

      if (maxShards.isDefined)
        ds.createSchema(sft, maxShards.get)
      else
        ds.createSchema(sft)


      if (ds.getSchema(featureName) != null) {
        logger.info(s"Feature '$featureName' on catalog table '$catalog' with spec " +
          s"'$spec' successfully created.")
      } else {
        logger.error(s"There was an error creating feature '$featureName' on catalog table " +
          s"'$catalog' with spec '$spec'. Please check that all arguments are correct " +
          "in the previous command.")
      }
    } else {
      logger.error(s"A feature named '$featureName' already exists in the data store with " +
        s"catalog table '$catalog'.")
    }
    
  }
  
  
}
