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

  dust.loadSource(dust.compile($('#tmpl-federationhealth').html(), 'federationhealth'));
  dust.loadSource(dust.compile($('#tmpl-namenode').html(), 'namenode-info'));
  dust.loadSource(dust.compile($('#tmpl-router').html(), 'router-info'));
  dust.loadSource(dust.compile($('#tmpl-mounttable').html(), 'mounttable'));

  $.fn.dataTable.ext.order['ng-value'] = function (settings, col)
  {
    return this.api().column(col, {order:'index'} ).nodes().map(function (td, i) {
      return $(td).attr('ng-value');
    });
  };

  function load_overview() {
    var BEANS = [
      {"name": "federation",  "url": "/jmx?qry=Hadoop:service=Router,name=FederationState"},
      {"name": "routerstat",  "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"},
      {"name": "router",      "url": "/jmx?qrt=Hadoop:service=NameNode,name=NameNodeInfo"},
      {"name": "mem",         "url": "/jmx?qry=java.lang:type=Memory"}
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
        return chunk.write('' + new Date(Number(value)).toLocaleString());
      }
    };

    var data = {};

    // Workarounds for the fact that JMXJsonServlet returns non-standard JSON strings
    function workaround(nn) {
      return nn;
    }

    load_json(
      BEANS,
      guard_with_startup_progress(function(d) {
        for (var k in d) {
          data[k] = k === 'federation' ? workaround(d[k].beans[0]) : d[k].beans[0];
        }
        render();
      }),
      function (url, jqxhr, text, err) {
        show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
      });

    function render() {
      var base = dust.makeBase(HELPERS);
      dust.render('federationhealth', base.push(data), function(err, out) {
        $('#tab-overview').html(out);
        $('#ui-tabs a[href="#tab-overview"]').tab('show');
      });
    }
  }

  function load_namenode_info() {
    var HELPERS = {
      'helper_lastcontact_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + new Date(Date.now()-1000*Number(value)));
      }
    };

    function workaround(r) {
      function node_map_to_array(nodes) {
        var res = [];
        for (var n in nodes) {
          var p = nodes[n];
          p.name = n;
          res.push(p);
        }
        return res;
      }

      function capitalise(string) {
          return string.charAt(0).toUpperCase() + string.slice(1).toLowerCase();
      }

      function augment_namenodes(nodes) {
        for (var i = 0, e = nodes.length; i < e; ++i) {
          var n = nodes[i];
          n.usedPercentage = Math.round(n.used * 1.0 / n.totalSpace * 100);
          n.title = "Unavailable";
          n.iconState = "unavailable";
          if (n.isSafeMode === true) {
            n.title = capitalise(n.state) + " (safe mode)"
            n.iconState = "safemode";
          } else if (n.state === "ACTIVE") {
            n.title = capitalise(n.state);
            n.iconState = "active";
          } else if (nodes[i].state === "STANDBY") {
            n.title = capitalise(n.state);
            n.iconState = "standby";
          } else if (nodes[i].state === "UNAVAILABLE") {
            n.title = capitalise(n.state);
            n.iconState = "unavailable";
          } else if (nodes[i].state === "DISABLED") {
            n.title = capitalise(n.state);
            n.iconState = "disabled";
          }
          if (n.namenodeId === "null") {
            n.namenodeId = "";
          }
        }
      }

      r.Nameservices = node_map_to_array(JSON.parse(r.Nameservices));
      augment_namenodes(r.Nameservices);
      r.Namenodes = node_map_to_array(JSON.parse(r.Namenodes));
      augment_namenodes(r.Namenodes);
      return r;
    }

    $.get(
      '/jmx?qry=Hadoop:service=Router,name=FederationState',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('namenode-info', base.push(data), function(err, out) {
          $('#tab-namenode').html(out);
          $('#ui-tabs a[href="#tab-namenode"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  function load_router_info() {
    var HELPERS = {
      'helper_lastcontact_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + new Date(Date.now()-1000*Number(value)));
      }
    };

    function workaround(r) {
      function node_map_to_array(nodes) {
        var res = [];
        for (var n in nodes) {
          var p = nodes[n];
          p.name = n;
          res.push(p);
        }
        return res;
      }

      function capitalise(string) {
          return string.charAt(0).toUpperCase() + string.slice(1).toLowerCase();
      }

      function augment_routers(nodes) {
        for (var i = 0, e = nodes.length; i < e; ++i) {
          var n = nodes[i];
          n.title = "Unavailable"
          n.iconState = "unavailable";
          if (n.status === "INITIALIZING") {
            n.title = capitalise(n.status);
            n.iconState = "active";
          } else if (n.status === "RUNNING") {
            n.title = capitalise(n.status);
            n.iconState = "active";
          } else if (n.status === "SAFEMODE") {
            n.title = capitalise(n.status);
            n.iconState = "safemode";
          } else if (n.status === "STOPPING") {
            n.title = capitalise(n.status);
            n.iconState = "unavailable";
          } else if (n.status === "SHUTDOWN") {
            n.title = capitalise(n.status);
            n.iconState = "unavailable";
          }
        }
      }

      r.Routers = node_map_to_array(JSON.parse(r.Routers));
      augment_routers(r.Routers);
      return r;
    }

    $.get(
      '/jmx?qry=Hadoop:service=Router,name=FederationState',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('router-info', base.push(data), function(err, out) {
          $('#tab-router').html(out);
          $('#ui-tabs a[href="#tab-router"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  function load_mount_table() {
    var HELPERS = {}

    function workaround(resource) {
      function augment_read_only(mountTable) {
        for (var i = 0, e = mountTable.length; i < e; ++i) {
          if (mountTable[i].readonly == true) {
            mountTable[i].readonly = "true"
          } else {
            mountTable[i].readonly = "false"
          }
        }
      }

      resource.MountTable = JSON.parse(resource.MountTable)
      augment_read_only(resource.MountTable)
      return resource;
    }

    $.get(
      '/jmx?qry=Hadoop:service=Router,name=FederationState',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('mounttable', base.push(data), function(err, out) {
          $('#tab-mounttable').html(out);
          $('#ui-tabs a[href="#tab-mounttable"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  function toTitleCase(str) {
    return str.replace(/\w\S*/g, function(txt){
        return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
      });
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
          show_err_msg('Router error: ' + err);
        }
      }
    };
  }

  function load_page() {
    var hash = window.location.hash;
    switch(hash) {
      case "#tab-overview":
        load_overview();
        break;
      case "#tab-namenode":
        load_namenode_info();
        break;
      case "#tab-router":
        load_router_info();
        break;
      case "#tab-mounttable":
        load_mount_table();
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
