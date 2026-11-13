// Entry point: start the client, tick the server clock once a second, render the app.
import { render } from "preact";
import { App, route } from "./app.tsx";
import { browserEnvironment, StateClient } from "./client.ts";
import "./app.css";

const client = new StateClient(browserEnvironment());
let started = false;
route.subscribe((current) => {
  if (current.kind !== "screen" || started) return;
  started = true;
  setInterval(() => client.tick(), 1000);
  void client.start();
});
render(<App client={client} />, document.body);
