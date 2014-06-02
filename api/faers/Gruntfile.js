// FAERS API Gruntfile

module.exports = function(grunt) {
  var path = require('path');

  grunt.loadNpmTasks('grunt-contrib-nodeunit');

  grunt.initConfig({
    nodeunit: {
      all: ['*_test.js'],
      options: {
        reporter: 'verbose'
      }
    }
  });
};
