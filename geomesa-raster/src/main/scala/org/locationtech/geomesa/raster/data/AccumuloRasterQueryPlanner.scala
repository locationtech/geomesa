package org.locationtech.geomesa.raster.data

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.index.QueryPlan
import org.locationtech.geomesa.utils.geohash.BoundingBox

// TODO: Constructor needs info to create Row Formatter
class AccumuloRasterQueryPlanner extends Logging {

  def getQueryPlan(rq: RasterQuery): QueryPlan = {

    // TODO: WCS: Improve this if possible
    // ticket is GEOMESA-560
    val hashes = BoundingBox.getGeoHashesFromBoundingBox(rq.bbox)
    logger.debug(s"Planner: BBox: ${rq.bbox} has geohashes: $hashes ")

    val res = rq.resolution

    val rows = hashes.map { gh =>
      // TODO: Use Row Formatter here
      // GEOMESA-555
      new org.apache.accumulo.core.data.Range(new Text(s"~$res~$gh"))
    }

    // TODO: WCS: Configure RasterFilteringIterator here for use in the QueryPlan
    // this will entail the generation of a Seq[IteratorSetting]
    // ticket is GEOMESA-558

    // TODO: WCS: setup a CFPlanner to match against a list of strings
    // ticket is GEOMESA-559
    QueryPlan(Seq(), rows, Seq())


  }
}
