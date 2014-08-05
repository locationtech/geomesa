# GeoMesa Tools

The command line tools project for GeoMesa.

## General Usage

# Commands: 

## create

## delete

## explain

## export


## ingest

Ingests TSV and CSV files containing WKT geometries with the following caveat:CSV files must surround values with double quotation marks, e.g.: `"37266103","2013-07-17","POINT(0.0 0.0)"` the first and last quotation marks are optional however. Also the WKT Geometry is assumed to be the last column of the CSV/TSV file.
####Usage
   
```ingest --file <> --format <> --table <> --typeName <> --spec <> --datetime <> --dtformat <> --method <>
```

note: *the `<>` marks are where user values would go*

with the following parameters:
     
`--file` The file path to the csv file or tsv file being ingested.

`--format` The format of that file, either CSV or TSV.

`--table` The accumulo table name, the table will be created if not already extant.

`--spec` The SimpleFeatureType of the CSV or TSV file, must match the layout of columns in the CSV/TSV file

`--datetime` The name of the field in the SFT spec above that corresponds to the the *time* column in the data being ingested.

`--method` The method of choice to perform the ingest, currently only `naive` is supported


## list