#!/usr/bin/node

var cheerio = require('cheerio');
var fs = require('fs');


const toText = function(elems) {
  var ret = '',
    len = elems.length,
    elem;

  for (var i = 0; i < len; i++) {
    elem = elems[i];
    if (elem.type === 'text' && elem.data.trim().length > 0) ret += (elem.data + ' ');
    else if (elem.children && elem.type !== 'comment') {
      ret += toText(elem.children);
    }
  }

  return ret;
};


ParseSections = function(sections_filepath) {
  var code_to_name = {};
  var csv = fs.readFileSync(sections_filepath, 'utf-8');
  var csv_split = csv.split('\n');
  for (i = 0; i < csv_split.length; i++) {
    var csv_line = csv_split[i].replace(/"/, '');
    var row = csv_line.split(',');

    var code = row[0];
    var name_for_json = row[1].replace(/:/, '').replace(/ & /g, ' and ');
    name_for_json = name_for_json.replace(/\//, ' or ');
    name_for_json = name_for_json.replace(/ /g, '_').toLowerCase();
    name_for_json = name_for_json.replace(/spl_unclassified/,
                                          'spl_unclassified_section');
    code_to_name[code] = name_for_json;
  }
  return code_to_name;
}

PopulateMetaDataFromXml = function(xml_filepath, output_json) {
  // Process xml for meta data fields
  xml = fs.readFileSync(xml_filepath, 'utf-8');
  $ = cheerio.load(xml, {
    normalizeWhitespace: true,
    xmlMode: true
  });

  output_json['set_id'] = $('setId').attr('root');
  output_json['id'] = $('id').attr('root');
  output_json['effective_time'] = $('effectiveTime').attr('value');
  output_json['version'] = $('versionNumber').attr('value');
  };

PopulateSectionsFromXml = function(xml_filepath, output_json) {
  // Process xml for the data from each section
  xml = fs.readFileSync(xml_filepath, 'utf-8');
  $ = cheerio.load(xml, {
    normalizeWhitespace: true,
    xmlMode: true
  });

  var previous_sections = [];

  $('section').each(function(i, section) {
    var code = 'spl_unclassified_section';
    var section_code = $(this).find('code').attr('code');
    if (section_code) {
      var section_name = code_to_name[section_code];
      if (section_name) {
        code = section_name;
      }
    }

    // For sections like recent_major_changes which have statements broken apart
    // by <br/>.
    $('br').replaceWith(' ');

    var is_subsection = $(this).parentsUntil($('section')).length == 1;

    // Only include subsections if they are classified. There's no reason
    // to duplicate the text otherwise.
    if (!(code == 'spl_unclassified_section' && is_subsection)) {
      if (output_json[code] == undefined) {
        output_json[code] = [];
      }
      var text = toText($(this)).trim().replace(/ +/gm, ' ');
      output_json[code].push(text);

      $(this).find('table').each(function(j, table) {
        var code_table = code + '_table';
        if (output_json[code_table] == undefined) {
          output_json[code_table] = [];
        }
        output_json[code_table].push($.html(table));
      });
    }
  });
};

var xml_filepath = process.argv[2];
var sections_path = process.argv[3];
var code_to_name = ParseSections(sections_path)
var output_json = {};
PopulateSectionsFromXml(xml_filepath, output_json);
PopulateMetaDataFromXml(xml_filepath, output_json)
console.log(JSON.stringify(output_json));
