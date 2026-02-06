const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const redis = require('ioredis');


const app = express();
const server = http.createServer(app);
app.use(express.json()); // Middleware to parse JSON in the request body

// CORS MIDDLEWARE
var allowCrossDomain = function (req, res, next) {
    res.header('Access-Control-Allow-Origin', "*");
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
}
app.use(allowCrossDomain);

/* REDIS */
const redisClient = redis.createClient({
    host: '127.0.0.1', // Redis server host
    port: 6379,        // Redis server port
});

redisClient.on('connect', () => {
    console.log('Connected to Redis');
});

redisClient.on('error', (err) => {
    console.error(`Redis Error: ${err}`);
});

// ADD CHANNEL
app.post('/channel', (req, res) => {
    const { id, name } = req.body;
    const result = redisClient.hmset(`channel:${id}`, 'id', id, 'name', name, (err) => {
        if (err) {
            res.status(500).json({ error: 'Failed to create channel' });
        } else {
            res.json({ message: 'Channel created successfully' });
        }
    });
});

// GET ALL
app.get('/channel', (req, res) => {
    const results = [];
    // Use the SCAN command to find keys matching the pattern
    redisClient.scan('0', 'MATCH', "channel:*", (err, reply) => {
        if (err) {
            res.status(500).json({ error: 'Failed to read channels' });
            return;
        }
        const keys = reply[ 1 ];

        keys.forEach((key) => {
            redisClient.hgetall(key, (err, channelData) => {
                if (err) {
                    res.status(500).json({ error: 'Failed to read channel' });
                } else {
                    results.push({ [ key ]: channelData });
                }

                if (results.length === keys.length) {
                    res.json(results);
                    return
                }
            })
        });
    })
});

// COUNT
app.get('/channel/count', (req, res) => {
    // Use the SCAN command to find keys matching the pattern
    redisClient.scan('0', 'MATCH', "channel:*", (err, reply) => {
        if (err) {
            res.status(500).json({ error: 'Failed to read channels' });
            return;
        }
        const keys = reply[ 1 ];
        const channelCount = keys.length;

        res.json({ channelCount });
    });
});

/* SOCKET */
const io = socketIo(server, { //create socket server
    cors: {
        origin: 'http://localhost:3000'
    }
});

const rooms = new Map(); // Map to store room information
var users = [];

// Function to be executed
const refresh = (socket) => {
    // Emit an acknowledgment or additional data if needed
    socket.emit('dataRefreshed', { message: 'Data refreshed on the server' });
};


io.on('connection', async (socket) => {

    console.log(`Socket Client connected: ${socket.id}`);

    socket.on('userConnect', (userName) => {
        users.push(userName)
        io.emit('connectedUsers', users)
    })

    socket.on('joinRoom', ({ roomName, userName }) => {

        socket.join(roomName); // Join the room 
        io.to(roomName).emit('userJoined', userName);
        console.log(`${userName} has join room ${roomName}`);

        // Add the user to the room data
        if (!rooms.has(roomName)) {
            rooms.set(roomName, []);
        }

        rooms.get(roomName).push(userName);
        const connectedUsers = rooms.get(roomName)
        console.log('all rooms join', [ ...rooms.entries() ]);
        io.to(roomName).emit('connectedUsersInRoom', connectedUsers);
        io.emit('connectedUsers', users);
    });

    socket.on('leftRoom', ({ roomName, userName }) => {
        socket.leave(roomName); // Leave the specified room
        io.to(roomName).emit('userLeft', userName);

        // Remove the user from the room data
        if (rooms.has(roomName)) {
            const userIndex = rooms.get(roomName).indexOf(userName);
            console.log('all rooms left', [ ...rooms.entries() ]);

            if (userIndex !== -1) {
                rooms.get(roomName).splice(userIndex, 1);
                const connectedUsers = rooms.get(roomName);
                console.log('connectedUsersInRoom', connectedUsers)
                io.to(roomName).emit('connectedUsersInRoom', connectedUsers);
                io.emit('connectedUsers', users);
            }
        }
    });

    // Handle incoming messages from clients
    socket.on('message', (data) => {
        // Broadcast the message to everyone in the room
        const { message, roomName } = data
        io.to(roomName).emit('message', message);
        console.log(`Received message: ${message} in room ${roomName}`);

    });

    // Handle refresh rooms data
    socket.on('refresh', () => {
        // Execute the refresh function when 'refresh' event is received
        refresh(socket);
    });

    // Handle user disconnection
    socket.on('userDisconnect', (us) => {
        disconnectUser(us)
    });

    // Handle socket client disconnection
    socket.on('disconnect', () => {
        console.log(`Socket Client disconnected: ${socket.id}`);
        socket.disconnect();


    });
});

const disconnectUser = (us) => {
    users.length = 0;
    users = [ ...us ]
    io.emit('connectedUsers', users)
}
server.listen(8080, () => {
    console.log('Server is listening on port 8080');
});
