'use strict';


module.exports = function(grunt) {

  // Project configuration.
  grunt.initConfig({
    watch: {
      sass: {
        files: 'src/**/*.sass',
        tasks: ['sass']
      },
      haml: {
        files: 'src/**/*.haml',
        tasks: ['haml']
      },
      coffee: {
        files: 'src/**/*.coffee',
        tasks: ['coffee']
      }
    },
    sass: {
      dist: {
        files: grunt.file.expandMapping(['src/sass/**/*.sass'], 'public/css/', {
          rename: function(destBase, destPath){
            return destBase+destPath.replace(/src\/sass\//, "").replace(/\.sass$/, ".css");
          }
        }) 
      }
    },
    haml: {
      dist: {
        files: grunt.file.expandMapping(['src/haml/**/*.haml'], 'public/', {
          rename: function(destBase, destPath){
            var extension = !!destPath.match(/.*\/templates\/.*/) ? "" : ".html"
            return destBase+destPath.replace(/src\/haml\//, "").replace(/\.haml$/, extension);
          }
        })
      }
    },
    coffee: {
      dist: {
        files: grunt.file.expandMapping(['src/coffee/**/*.coffee'], '', {
          rename: function(destBase, destPath){
            if(destPath==='src/coffee/server.coffee') { return 'server.js'; }
            if(destPath.indexOf('/server/') !== -1) {
              return destPath.replace(/src\/coffee\/server\//, 'lib/').replace(/\.coffee$/, '.js');
            }
            if(destPath.indexOf('/common/') !== -1) {
              return destPath.replace(/src\/coffee\/common\//, 'public/js/').replace(/\.coffee$/, '.js');
            }
          }
        })
      }
    }
  });

  // These plugins provide necessary tasks.
  // grunt.loadNpmTasks('grunt-contrib-nodeunit');
  // grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-sass');
  grunt.loadNpmTasks('grunt-contrib-haml');
  grunt.loadNpmTasks('grunt-contrib-coffee');

  // Default task.
  grunt.registerTask('default', ['watch']);

};
