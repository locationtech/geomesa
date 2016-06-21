# Twitter

## Getting Twitter Data

Unlike other example data sets, the Twitter data set does not have an immediately downloadable link. Instead, one must use the [Twitter API](https://dev.twitter.com/rest/public) to receive data as JSON.

## Cleaning the Data

The converter expects data is in JSON that has had newlines removed, such that there is one record per line. Files may be compressed.

## Geometry

Twitter data collected with location data may have a point location if the user posts with precise location. This is stored in the `coordinates` field. Otherwise the tweet is associated with a named place, which has a bounding box. 

The bounding box provided by the Twitter API is not a properly formed geoJson polygon. The array of points does not form a linear ring, as it does not close. Thus the converter takes the bounds and builds a polygon from it. The centroid of this box is then taken as a point geometry for the tweet.

## Ingest procedure

Check that `twitter` simple feature type is available on the GeoMesa tools classpath. This is the default case.

    geomesa env | grep twitter

If it is not, merge the contents of `reference.conf` with `$GEOMESA_HOME/conf/application.conf`, or ensure that `reference.conf` is in `$GEOMESA_HOME/conf/sfts/twitter`

A recommended ingest procedure is to ingest first picking up the bounding boxes. Tweets with point geometry may fail this ingest. Then ingest to pick up the points. Tweets without points will fail this ingest, and their geometries will remain as set in the first pass.

    geomesa ingest -u USERNAME -c CATALOG -s twitter -C twitter-place-centroid hdfs://namenode:port/path/to/twitter/*
    geomesa ingest -u USERNAME -c CATALOG -s twitter -C twitter hdfs://namenode:port/path/to/twitter/*
