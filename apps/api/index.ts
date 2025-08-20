import { ChromaClient } from "chromadb";
import express from "express";

const port = 3000;

const app = express();

const client = new ChromaClient();
// console.log("created", client);

app.get("/version", (req, res) => {
  res.send({ version: "0.1.0", health: "ok" });
});

app.get("/db/heartbeat", async (req, res) => {
  const heartbeat = await client.heartbeat();
  res.send({
    heartbeat,
  });
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
