/*
 * Copyright (c) 2013, Circonus, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials provided
 *       with the distribution.
 *     * Neither the name Circonus, Inc. nor the names of its contributors
 *       may be used to endorse or promote products derived from this
 *       software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. */

var uuid = require('libuuid'),
    os = require('os'),
    dgram = require('dgram'),
    util = require('util'),
    events = require('events');

function NetHeartBeat(app, port, addr) {
  this.app = app;
  this.port = port;
  this.addr = addr;
  this.ident = uuid.create() + ":" + os.hostname();
  this.nodes = {};
  this.leader = null;
  this._isLeader = false;
}
util.inherits(NetHeartBeat, events.EventEmitter);

NetHeartBeat.prototype.finishmc = function() {
  var self = this;
  self.socket.setBroadcast(true);
  self.socket.setMulticastTTL(128);
  self.socket.setMulticastLoopback(true);
  self.socket.addMembership(self.addr);
  self.hb = setInterval(function() { self.beat() }, self.period);
}
NetHeartBeat.prototype.finishbc = function() {
  var self = this;
  self.socket.setBroadcast(true);
  self.hb = setInterval(function() { self.beat() }, self.period);
}
NetHeartBeat.prototype.start = function(period, min_skew, max_age) {
  var self = this;
  self.period = period || 500;
  self.min_skew = min_skew || 5000;
  self.max_age = max_age || 2000;
  self.socket = dgram.createSocket("udp4"); 
  self.socket.on('error', function(err) { self.emit('error', err); });
  self.socket.on('message', function(msg, rinfo) { self.process(msg, rinfo); });

  var foct = self.addr.split('.')[0];
  if(foct >= 224 && foct < 240) {
    self.socket.bind(self.port);
    self.socket.on('listening', function() { self.finishmc(); });
  }
  else {
    self.socket.bind(self.port, self.addr);
    self.socket.on('listening', function() { self.finishbc(); });
  }
  self.bootTime = +(new Date());
};

NetHeartBeat.prototype.getHeader = function() {
  return "[NetHeartBeat-" + this.app + "]";
}

NetHeartBeat.prototype.beat = function() {
  var message = this.getHeader() + this.bootTime + ":" + this.ident;
  var payload = new Buffer(message);
  this.socket.send(payload, 0, payload.length, this.port, this.addr, function(err, bytes) {
  });
}

NetHeartBeat.prototype.process = function(payload, rinfo) {
  var message = payload.toString();
  var hdr = this.getHeader();
  if(message.substr(0,hdr.length) == hdr) {
    message = message.substr(hdr.length);
    var parts = message.match(/^([^:]+):(.+)/);
    if(parts != null) {
      var info = { lastReceived: +(new Date()),
                   bootTime: parseInt(parts[1]),
                   ident: parts[2] };
      this.nodes[parts[2]] = info;
      this.elect();
    }
  }
}

NetHeartBeat.prototype.elect = function() {
  var now = +(new Date());
  var nl = null;
  for (var name in this.nodes) {
    var info = this.nodes[name];
    if(nl == null) nl = info;
    // If it's old, toss it.
    if(now - info.lastReceived > this.max_age)
      delete this.nodes[name];
    // If it isn't us and the boot times aren't skewed enough, help.
    else if(this.ident != info.ident &&
            this.bootTime >= info.bootTime &&
            (this.bootTime - info.bootTime) < this.min_skew)
      this.bootTime += Math.floor((2 * Math.random() * this.min_skew));
    // choose the oldest.
    else if(info.bootTime < nl.bootTime) nl = info;
  }
  if(now - nl.bootTime > this.max_age) {
    if(nl.ident != this.leader) {
      var old_leader = this.leader;
      this.leader = nl.ident;
      this._isLeader = (this.leader == this.ident);
      this.emit('leaderChanged', this.leader, old_leader);
    }
  }
}

NetHeartBeat.prototype.isLeader = function() {
  return this._isLeader;
}

module.exports = NetHeartBeat;
