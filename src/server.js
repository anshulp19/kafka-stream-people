const WebSocket = require('ws');
const express = require('express');
const { kafkaSubscribe } = require('./consumer');

const PORT = parseInt(process.env.PORT) || 3210;

const app = express();

// Server static files
app.use(express.static('./'));

const server = new WebSocket.Server({ server: app.listen(PORT) });

/* const send = async (message) => {
  try {
    console.log("send message: " + JSON.stringify(message));
    server.clients.forEach(
    async (client) => {
        
      await client.send([message]);
    }
  );
  } catch(err) {
      console.error(err);
  }
} */

server.on(
  'connection', async (ws)=> {
      try {
        await kafkaSubscribe(async (data)=> {
            console.log("data: " + JSON.stringify(data));
            await ws.send(JSON.stringify(data));
        });
      } catch(err) {
        console.error(err);
      }; 
   }
);

console.log(`Server listening: http://localhost:${PORT}`);