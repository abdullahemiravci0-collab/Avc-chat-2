const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
const PORT = process.env.PORT || 3000;

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/avcichat';
mongoose.connect(MONGODB_URI)
  .then(() => console.log('✅ MongoDB Bağlantısı Başarılı'))
  .catch(err => console.log('❌ MongoDB Hatası:', err));

const Message = mongoose.model('Message', new mongoose.Schema({
  from: String,
  to: String,
  text: String,
  time: { type: Date, default: Date.now }
}));

const User = mongoose.model('User', new mongoose.Schema({
  username: { type: String, unique: true },
  lastSeen: { type: Date, default: Date.now }
}));

app.use(express.static(path.join(__dirname, 'public')));
const onlineUsers = {};

io.on('connection', (socket) => {
  socket.on('register', async (username) => {
    onlineUsers[username] = socket.id;
    socket.username = username;
    await User.findOneAndUpdate({ username }, { lastSeen: Date.now() }, { upsert: true });
    io.emit('users', Object.keys(onlineUsers));
  });

  socket.on('private message', async ({ to, text }) => {
    const msg = new Message({ from: socket.username, to, text });
    await msg.save();
    const toSocket = onlineUsers[to];
    if (toSocket) io.to(toSocket).emit('private message', msg);
    socket.emit('private message', msg);
  });

  socket.on('get history', async ({ withUser }, cb) => {
    const history = await Message.find({
      $or: [
        { from: socket.username, to: withUser },
        { from: withUser, to: socket.username }
      ]
    }).sort({ time: 1 });
    cb(history);
  });

  socket.on('call-user', ({ to, offer, type }) => {
    const toSocket = onlineUsers[to];
    if (toSocket) io.to(toSocket).emit('incoming-call', { from: socket.username, offer, type });
  });

  socket.on('answer-call', ({ to, answer }) => {
    const toSocket = onlineUsers[to];
    if (toSocket) io.to(toSocket).emit('call-answered', { answer });
  });

  socket.on('ice-candidate', ({ to, candidate }) => {
    const toSocket = onlineUsers[to];
    if (toSocket) io.to(toSocket).emit('ice-candidate', { candidate });
  });

  socket.on('end-call', ({ to }) => {
    const toSocket = onlineUsers[to];
    if (toSocket) io.to(toSocket).emit('call-ended');
  });

  socket.on('reject-call', ({ to }) => {
    const toSocket = onlineUsers[to];
    if (toSocket) io.to(toSocket).emit('call-rejected');
  });

  socket.on('disconnect', async () => {
    if (socket.username) {
      delete onlineUsers[socket.username];
      await User.findOneAndUpdate({ username: socket.username }, { lastSeen: Date.now() });
      io.emit('users', Object.keys(onlineUsers));
    }
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`🏹 Avcı Chat Sunucusu Hazır: Port ${PORT}`));
