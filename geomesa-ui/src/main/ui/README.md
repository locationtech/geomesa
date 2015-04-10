# Geomesa UI

## Before getting started
### Recommended to use [Nvm](https://github.com/creationix/nvm) for node 
* Ubuntu
1. sudo apt-get update
1. sudo apt-get install build-essential libssl-dev
1. curl https://raw.githubusercontent.com/creationix/nvm/v0.24.1/install.sh | sh
1. source ~/.profile
1. nvm install v0.10 #this will install eg v0.10.38
1. nvm use v0.10
1. node -v
>v.0.10.38

### Tools before getting started
1. sudo npm -g install grunt-cli
1. sudo npm -g install bower
1. sudo npm -g install karma

## Getting started
1. npm install
1. bower install
1. grunt server
1. enjoy by using default localhost 9090

* ps: the first two commands may require sudo to install package and lib using npm and bower 

## Documentation
**Make wps request**
	
* **a. Get a xml request file to query geomesa with.**
		
>1. In browser, go to 104.130.224.117:8080/geoserver.
>
>1. At bottom left, click Demos.
>		
>1. Click WPS Request Builder
>
>1. Under the ‘Choose Process’ dropdown menu, there are a few WPS processes prefixed by “geomesa:”
>
>1. Choose ‘geomesa:Unique’ for example.
>
>1. For attribute use ‘When’ for example. (For a list of the attributes our quickstart data has, click on a data point in our UI and view it in the card).
>
>1. If you want to use a CQL/ECQL filter, you can find an example in many of the test cases in the Geomesa repo. For example, here We can find “dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z'” as one sample CQL filter. We would need to replace dtg to our time attribute, When. So, “When BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z'”
>
>1. Put ASC or DESC in that one box.
>
>1. Click generate XML, and save the xml file (name it unique.xml for example).


* **b. Send xml request to Geoserver Instance on our cloud. (Geoserver is hooked up to Geomesa).**
>1. You can find some sample xml request files in the subdirectories found here. Some of the sample xml request files have a comment like this:
>
>><!-- curl -u admin:geoserver -H 'Content-type: xml' -XPOST -d@'KNNAboutWhiteHouse.xml' http://localhost:8080/geoserver/wps | json_pp -->
>
>2. Replace localhost:8080 with geomesa:8080
>
>1. Replace ‘KNNAboutWhiteHouse.xml’ with your xml request filename.
>
>1. Run the command in bash, for example 
>>curl -u admin:geoserver -H 'Content-type: xml' -XPOST -d@'unique.xml' http://localhost:8080/geoserver/wps | json_pp
>
>5. If you get an error message (or the return type for your request isn’t json), take out         ‘ | json_pp’) from the command. This way you will get the original error message instead of json_pp’s error message.


## Example of CQL statement
> When between '2014-01-31T14:24:11.000Z' AND '2014-01-31T18:24:11.000Z'

## Specs
* OpenLayers 3
* AngularJS
* Node.js
* Bootstrap 3