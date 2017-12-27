
# Global Database of Events, Language, and Tone (GDELT) for GeoMesa

This directory provides [GDELT](http://gdeltproject.org/) GeoMesa ingest commands and converter configuration files.

GDELT Event Database periodically scans news articles and uses natural language processing to identify  "the people, locations, organizations, counts, themes, sources, emotions, counts, quotes and events driving our global society every second of every day."

GDELT data is updated each morning at 6am. The ingester has last been tested on May 23rd 2016's dataset.

This readme describes the full process from original source data to GeoMesa ingest.

## Getting GDELT data
The GDELT data set can be downloaded using the provided ```download-data.sh``` script in `$GEOMESA_HOME/bin/` as such

    ./download-data.sh gdelt

Alternatively, download the GDELT from [the GDELT events page,](http://data.gdeltproject.org/events/index.html) select the zip file for the desired day, then `unzip` this in a convenient directory. This will result in a single CSV file.

Be aware that these files are actually tab-delimited, but are given the CSV extension for compatibility purposes. For this reason, be careful when modifying and saving these files in software like Excel as commas may be automatically inserted, breaking ingest functionality due to records like "Baltimore, Maryland". 

For more information on features of the GDELT data, see their documentation [here.](http://www.gdeltproject.org/data.html#documentation)

## GDELT Versions

GDELT has two different data formats, the original 1.0 and the new 2.0. GeoMesa supports both formats, with the
named simple feature types `gdelt` and `gdelt2`, respectively.

## Ingest Commands

Check that `gdelt` simple feature type is available on the GeoMesa tools classpath. This is the default case.

    geomesa env | grep gdelt

If it is not, merge the contents of `reference.conf` to `$GEOMESA_HOME/conf/application.conf`, or ensure that `reference.conf` is in `$GEOMESA_HOME/conf/sfts/gdelt`

Run the ingest. You may optionally point to a different accumulo instance using `-i` and `-z` options. See `geomesa help ingest` for more detail.

    geomesa ingest -u USERNAME -c CATALOGNAME -s gdelt -C gdelt gdelt_data.csv

Further be aware that any errors in ingestion will be logged to `$GEOMESA_HOME/logs`
