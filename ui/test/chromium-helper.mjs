import { createServer } from "node:net";

const server = createServer((socket) => {
  // One-byte commands and replies avoid TCP chunk framing.
  socket.on("data", (data) => {
    for (const command of data) {
      if (command === 33) process.exit(0);
      else if (command === 63) socket.write("A");
      else throw new Error("unknown helper command");
    }
  });
});
server.listen(0, "127.0.0.1", () => process.send({ pid: process.pid, port: server.address().port }));
