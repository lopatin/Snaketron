// ref: http://stackoverflow.com/questions/9887512/precompiled-handlebars-templates-in-backbone-with-requirejs
// hbtemplate.js plugin for requirejs / text.js
// it loads and compiles Handlebars templates
define(['handlebars'],
function (Handlebars) {

    var loadResource = function (resourceName, parentRequire, callback, config) {
        parentRequire([("text!" + resourceName)],
            function (templateContent) {
                var template = Handlebars.compile(templateContent);
                callback(template);
            }
        );
    };

    return {
        load: loadResource
    };

});
