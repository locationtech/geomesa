# Global Terrorism Database (GTD) for GeoMesa

This directory provides [GTD](http://www.start.umd.edu/gtd/) GeoMesa ingest commands, converter configuration files, and an R script to prepare data from the spreadsheet in which it is distributed.

This work is based on a GTD pull done 2015 December 17. The GTD data distribution is dated June 26, 2015. GTD data is updated annually.

This readme describes the full process from original source data to GeoMesa ingest. 

## Getting GTD data
Download the GTD from (http://www.start.umd.edu) via the contact link. You must fill out a web form. Select the zip file with all data and documents. Then `unzip` this in a convenient directory. This will result in a number of Excel (.xlsx) spreadsheets and PDF documents.

## R script
The R script extracts the GTD data from the Excel spreadsheets for a selection of the about 150 fields available. The R script expects two arguments. The first is the working directory, which is where the CSV files will be output. The second is the path to the GTD spreadsheet, relative to the working directory.

The R script extracts the data from the main spreadsheet into a `data.frame`. The script will then export a subset of columns to a file `gtd-include.csv`. For convenience, the column names in that file are printed to `gtd-column-names.csv`. Note there are ~160 available attributes.

The script then handles some data cleaning that *could* be dealt with by the geomesa inges: removing entries with invalid or missing dates and coordinates. The R script writes this `data.frame` to a CSV file `gtd-clean.csv`. This step results in about 18% of the data being dropped from the dataset.

## Ingest Commands

Check that `gtd` simple feature type is available on the GeoMesa tools classpath. This is the default case.

    geomesa env | grep gtd

If it is not, merge the contents of `reference.conf` with `$GEOMESA_HOME/conf/application.conf`, or ensure that `reference.conf` is in `$GEOMESA_HOME/conf/sfts/gtd`

Run the ingest. You may optionally point to a different accumulo instance using `-i` and `-z` options. See `geomesa help ingest` for more detail.

    geomesa ingest -u USERNAME -c CATALOGNAME -s gtd -C gtd gtd-clean.csv
