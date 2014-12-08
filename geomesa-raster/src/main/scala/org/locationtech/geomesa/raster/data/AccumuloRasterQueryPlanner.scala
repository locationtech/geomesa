package org.locationtech.geomesa.raster.data

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.index.QueryPlan
import org.locationtech.geomesa.utils.geohash.BoundingBox

class AccumuloRasterQueryPlanner extends Logging {

  def getQueryPlan(rq: RasterQuery): QueryPlan = {
    val hashes = BoundingBox.getGeoHashesFromBoundingBox(rq.bbox)
    logger.debug(s"Planner: BBox: ${rq.bbox} has geohashes: $hashes ")
    val res = rq.resolution

    val rows = hashes.map { gh =>
      new org.apache.accumulo.core.data.Range(new Text(s"~$res~$gh"))
    }

    // TODO: Configure Iterators and any ColumnFamilies
    QueryPlan(Seq(), rows, Seq())
  }
}
