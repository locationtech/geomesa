package geomesa.core.data

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchScanner, Scanner}
import org.opengis.feature.simple.SimpleFeatureType

trait AccumuloConnectorCreator extends Logging {

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   *
   * @param numThreads number of threads for the BatchScanner
   */
  def createSpatioTemporalIdxScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   */
  def createSTIdxScanner(sft: SimpleFeatureType): BatchScanner

  /**
   * Create a Scanner for the Attribute Table (Inverted Index Table)
   */
  def createAttrIdxScanner(sft: SimpleFeatureType): Scanner

  /**
   * Create a BatchScanner to retrieve only Records (SimpleFeatures)
   */
  def createRecordScanner(sft: SimpleFeatureType, numThreads: Int = 0) : BatchScanner

  def catalogTableFormat(sft: SimpleFeatureType): Boolean
}
