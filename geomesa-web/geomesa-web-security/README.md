## GeoMesa Web Security

GeoMesa Web Security provides Accumulo security to layers within GeoServer, particularly the KafkaDataStore but
it works for all layers.  To deploy the module, copy the ```geomesa-web-security-$VERSION.jar``` and dependencies to 
```$GEOSERVER_WEB_APP_HOME/WEB-INF/lib``` and restart GeoServer.  Then, ensure that layers are secured with 
read-only access.  The plugin will be triggered when a layer is accessed by a user that has read-only authorizations
and will apply Accumulo visibility filters to SimpleFeatures that have a visibility property in the ```user data```
hash map.  See ```org.locationtech.geomesa.core.security.SecurityUtils``` for more details on how to get and set
visibilities on a SimpleFeature
