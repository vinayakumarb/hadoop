/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function () {
  "use strict";

  dust.loadSource(dust.compile($('#tmpl-dfshealth').html(), 'dfshealth'));
  dust.loadSource(dust.compile($('#tmpl-startup-progress').html(), 'startup-progress'));
  dust.loadSource(dust.compile($('#tmpl-snapshot').html(), 'snapshot-info'));

  $.fn.dataTable.ext.order['ng-value'] = function (settings, col)
  {
    return this.api().column(col, {order:'index'} ).nodes().map(function (td, i) {
      return $(td).attr('ng-value');
    });
  };

  function load_overview() {
    var BEANS = [
      {"name": "nn",      "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"},
      {"name": "nnstat",  "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"},
      {"name": "fs",      "url": "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"},
      {"name": "fsn",     "url": "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"},
      {"name": "mem",     "url": "/jmx?qry=java.lang:type=Memory"}
    ];

    var HELPERS = {
      'helper_fs_max_objects': function (chunk, ctx, bodies, params) {
        var o = ctx.current();
        if (o.MaxObjects > 0) {
          chunk.write('(' + Math.round((o.FilesTotal + o.BlockTotal) / o.MaxObjects * 100) * 100 + ')%');
        }
      },

      'helper_dir_status': function (chunk, ctx, bodies, params) {
        var j = ctx.current();
        for (var i in j) {
          chunk.write('<tr><td>' + i + '</td><td>' + j[i] + '</td><td>' + params.type + '</td></tr>');
        }
      },

      'helper_date_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + moment(Number(value)).format('ddd MMM DD HH:mm:ss ZZ YYYY'));
      }
    };

    var data = {};

    $.ajax({'url': '/conf', 'dataType': 'xml', 'async': false}).done(
      function(d) {
        var $xml = $(d);
        var namespace, nnId;
        $xml.find('property').each(function(idx,v) {
          if ($(v).find('name').text() === 'dfs.nameservice.id') {
            namespace = $(v).find('value').text();
          }
          if ($(v).find('name').text() === 'dfs.ha.namenode.id') {
            nnId = $(v).find('value').text();
          }
        });
        if (namespace && nnId) {
          data['HAInfo'] = {"Namespace": namespace, "NamenodeID": nnId};
        }
    });

    // Workarounds for the fact that JMXJsonServlet returns non-standard JSON strings
    function workaround(nn) {
      nn.JournalTransactionInfo = JSON.parse(nn.JournalTransactionInfo);
      nn.NameJournalStatus = JSON.parse(nn.NameJournalStatus);
      nn.NameDirStatuses = JSON.parse(nn.NameDirStatuses);
      return nn;
    }

    load_json(
      BEANS,
      guard_with_startup_progress(function(d) {
        for (var k in d) {
          data[k] = k === 'nn' ? workaround(d[k].beans[0]) : d[k].beans[0];
        }

        data.fs.ObjectsTotal = data.fs.FilesTotal + data.fs.BlocksTotal;

        render();
      }),
      function (url, jqxhr, text, err) {
        show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
      });

    function render() {
      var base = dust.makeBase(HELPERS);
      dust.render('dfshealth', base.push(data), function(err, out) {
        $('#tab-overview').html(out);
        $('#ui-tabs a[href="#tab-overview"]').tab('show');
      });
    }
  }

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  function ajax_error_handler(url, jqxhr, text, err) {
    show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
  }

  function guard_with_startup_progress(fn) {
    return function() {
      try {
        fn.apply(this, arguments);
      } catch (err) {
        if (err instanceof TypeError) {
          show_err_msg('NameNode is still loading. Redirecting to the Startup Progress page.');
          load_startup_progress();
        }
      }
    };
  }

  function load_startup_progress() {
    function workaround(r) {
      function rename_property(o, s, d) {
        if (o[s] !== undefined) {
          o[d] = o[s];
          delete o[s];
        }
      }
      r.percentComplete *= 100;
      $.each(r.phases, function (idx, p) {
        p.percentComplete *= 100;
        $.each(p.steps, function (idx2, s) {
          s.percentComplete *= 100;
          // dust.js is confused by these optional keys in nested
          // structure, rename them
          rename_property(s, "desc", "stepDesc");
          rename_property(s, "file", "stepFile");
          rename_property(s, "size", "stepSize");
        });
      });
      return r;
    }
    $.get('/startupProgress', function (resp) {
      var data = workaround(resp);
      dust.render('startup-progress', data, function(err, out) {
        $('#tab-startup-progress').html(out);
        $('#ui-tabs a[href="#tab-startup-progress"]').tab('show');
      });
    }).error(ajax_error_handler);
  }

  function load_snapshot_info() {
    $.get(
      '/jmx?qry=Hadoop:service=NameNode,name=SnapshotInfo',
      guard_with_startup_progress(function (resp) {
      dust.render('snapshot-info', resp.beans[0], function(err, out) {
          $('#tab-snapshot').html(out);
          $('#ui-tabs a[href="#tab-snapshot"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  function load_page() {
    var hash = window.location.hash;
    switch(hash) {
      case "#tab-snapshot":
        load_snapshot_info();
        break;
      case "#tab-startup-progress":
        load_startup_progress();
        break;
      case "#tab-overview":
        load_overview();
        break;
      default:
        window.location.hash = "tab-overview";
        break;
    }
  }
  load_page();

  $(window).bind('hashchange', function () {
    load_page();
  });
})();
