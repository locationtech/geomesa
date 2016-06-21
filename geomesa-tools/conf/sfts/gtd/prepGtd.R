 #### Global Terrorism Database ####

# This script prepares the global terrorism database download for ingest into geomesa.
# Intent is to let the geomesa converters handle as much as possible and keep this script small in scope.

# 2015 December 17, GTD distribution dated June 26, 2015

# Prerequisites: 
#   Download the GTD from http://www.start.umd.edu
#   This is via the contact link, you must fill out a web form. 
#   Select the zip file with all data and documents.
#   `unzip` this.

# Post-requisite: cloud with geomesa stack

#### Prep environment ####
if(!require(openxlsx)) {
    install.packages('openxlsx')
    library(openxlsx)
}

#get arguments from the command line
args <- commandArgs(TRUE)
setwd(args[1]) # first argument is where to output files
fpath <- args[2] # second argument is path to XLSX file

#### Extract from Excel into data.frame #### 
# June 2015 version of the zip contains a file with all events plus subsets by time. Ignore the subsets and just use the master file

# Read the data, this takes up to a minute.
gtd <- read.xlsx(fpath, sheet = 1, startRow = 1, colNames = TRUE, check.names = TRUE, detectDates = TRUE)

includeCols <- c('eventid', 'iyear', 'imonth', 'iday', 'latitude', 'longitude', 'attacktype1_txt', 
                 'target1', 'gname', 'claimed', 'weaptype1_txt', 'nkill', 'nwound', 'propvalue')
write.table(gtd[includeCols], 'gtd-include.csv', sep = ',', 
            row.names = FALSE, col.names = FALSE, na = '')
write.csv(includeCols, 'gtd-column-names.csv')

# Checksum the iyear column. In LibreOffice Calc I get 283783785
sum(gtd$iyear)

# Basic check that everything is in
nrow(gtd)
ncol(gtd)
names(gtd)


#### Cleaning and munging ####
# Missing entries in the latitude and longitude caused those columns to be read as character

# Many other columns contain coded values for missing, see the code book included in the download.  Example: if day (of the month) is unknown it is recorded as a 0. Many of these codings are for categorical variables such that the category is "Unknown". This phenonmenon is a much bigger concern for time and location (indexed); numerical fields that may be used in analytics (e.g. number of perpetrators nperps coded as -99, but with -9 and NA also appearing.

## As a first cut just go with "complete" cases for indexed fields {date, location}  
gtdIx <- gtd
gtdIx[gtdIx$imonth == 0, 'imonth'] <- NA
gtdIx[gtdIx$iday == 0, 'iday'] <- NA

ccindex <- complete.cases(gtd[c('imonth', 'iday', 'latitude', 'longitude')])
#expect this many records to be written to gtd-clean.csv
sum(ccindex)

write.table(gtdIx[ccindex, includeCols], 'gtd-clean.csv', sep = ',', 
            row.names = FALSE, col.names = FALSE, na = '')

