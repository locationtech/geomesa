angular.module('geomesa.unique', [])

    .controller('UniqueController', ['$scope', function($scope) {
        var request = createWPSExecuteRequest('Who', 'true', 'ASC', 'true');
        OpenLayers.Request.POST({
            url: 'http://geomesa:8080/geoserver/wps',
            data: request,
            success: function(outputs){
                //For now just log results to console.
                console.log(outputs.responseText);
            }
        });   
    }]);

function createWPSExecuteRequest(attribute, histogram, sort, sortByCount) {
    var request = OpenLayers.Format.XML.prototype.write
                (new OpenLayers.Format.WPSExecute().writeNode('wps:Execute', {   

        identifier: 'geomesa:Unique',
        dataInputs: [{
            identifier: 'features',
            reference: {
                mimeType: 'text/xml',
                href: "http://geoserver/wfs",
                method: 'POST',
                body: {
                    wfs: {
                        service: "WFS",
                        version: "1.0.0",
                        outputFormat: "GML2",
                        featurePrefix: "geomesa",
                        featureType: "QuickStart",
                        featureNS: "http://geomesa.org/"
                    }
                }
            }},
            { identifier: 'attribute',
            data: {
                literalData: {
                    value: attribute
                } 
            }},
            {identifier: 'histogram',
            data: {
                literalData: {
                    value: histogram
                }
            }},
            {identifier: 'sort',
            data: {
                literalData: {
                    value: sort
                }
            }},
            {identifier: 'sortByCount',
            data: {
                literalData: {
                    value: sortByCount
                }
            }}
        ],
        responseForm: {
            rawDataOutput: {
                mimeType: "text/xml; subtype=wfs-collection/1.0",
                identifier: 'result'
            }
        }
    }));
    return request.replace("xlink:", "");
}
