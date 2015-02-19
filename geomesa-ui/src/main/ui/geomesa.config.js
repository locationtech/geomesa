// Project configuration.
//
module.exports = {
    
    // Where the project is build during development.
    //buildDir: '../../../../../../tools/tomcat7/webapps/dist/',
  
    buildDir: '../../../dist',

    // The location of the html page that will be built.
    // Change if needed...
    indexDest: 'index.html',

    // Where project is compiled for deployment.
    compileDir: 'deploy',

    // App file patterns used by the build system.
    appFiles: {
        js : [ 'src/**/*.js', '!src/**/*.spec.js', '!src/assets/**/*.js' ],
        specs: [ 'src/**/*.spec.js' ],
        tpl: [ 'src/app/**/*.tpl.html' ],
        html: [ 'src/index.html' ],
        stylus: 'src/stylus/main.styl'
    },
    
    // Dependencies needed for tests.
    testFiles: {
        js: [
            'vendor/bower/angular-mocks/angular-mocks.js'
        ]
    },

    // A list of vendor files needed for development. As you add new libraries
    // add a path to the files here. The 'index' task will append script tag
    // for them to 'indexDest' file specified above.
    vendorFiles: {
        js: [
            'vendor/bower/d3/d3.js',
            'vendor/bower/lodash/dist/lodash.js',
            'vendor/bower/jquery/dist/jquery.js',
            'vendor/bower/angular/angular.js',
            'vendor/bower/angular-route/angular-route.js',
            'vendor/bower/angular-resource/angular-resource.js',
            'vendor/bower/moment/moment.js',
            'vendor/bower/moment-timezone/builds/moment-timezone-with-data-2010-2020.min.js',
            'vendor/bower/leaflet-dist/leaflet.js',
            'vendor/bower/leaflet-providers/leaflet-providers.js',
            'vendor/bower/openlayers/build/ol.js',
            // TODO - test only
            'vendor/bower/angular-mocks/angular-mocks.js',
        ],
        css: ['vendor/bower/openlayers/css/ol.css'],
        assets: [
            'vendor/bower/font-awesome/fonts/*',
            'vendor/bower/bootstrap/fonts/*'
        ]
    }
}
