package geomesa.core.util

import geomesa.core.data.AccumuloDataStore
import org.apache.accumulo.core.client.{Scanner, BatchScanner}
import org.opengis.feature.simple.SimpleFeatureType

class ExplainingDataStore(output: String => Unit = println) extends AccumuloDataStore(null, null, null, null) {
  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   *
   * @param numThreads number of threads for the BatchScanner
   */
  override def createSpatioTemporalIdxScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner = new ExplainingBatchScanner(output)

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   */
  override def createSTIdxScanner(sft: SimpleFeatureType): BatchScanner = new ExplainingBatchScanner(output)

  /**
   * Create a Scanner for the Attribute Table (Inverted Index Table)
   */
  override def createAttrIdxScanner(sft: SimpleFeatureType): Scanner = new ExplainingScanner(output)

  /**
   * Create a BatchScanner to retrieve only Records (SimpleFeatures)
   */
  override def createRecordScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner = new ExplainingBatchScanner(output)
}
