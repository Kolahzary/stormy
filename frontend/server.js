var app = require('express').createServer()
  , io = require('socket.io').listen(app);

app.listen(8080);

app.get('/', function(req, res) {
  var lat = Number(req.param('lat'));
  var lon = Number(req.param('lon'));
  if (lat && lon) {
    emitAll('coordinate', {lat: lat, lon: lon});
  }
  res.send(200);
});

var _socks = [];

function emitAll(eventName, data) {
  _socks.forEach(function(sock) {
    sock.emit(eventName, data);
  })
}

io.sockets.on('connection', function(socket) {
  _socks.push(socket);
  socket.on('disconnect', function() {
    var index = _socks.indexOf(socket);
    if (index >= 0) _socks.splice(index, 1);
  });
});

