import { config } from 'dotenv';

config();

import { createHTTPServer } from '@trpc/server/adapters/standalone';
import { appRouter } from './routers/_app';
import { createContext } from './trpc';

const server = createHTTPServer({
  router: appRouter,
  createContext
});

const HTTP_SERVER_PORT = process.env.HTTP_SERVER_PORT;

if (!HTTP_SERVER_PORT) {
  throw new Error('HTTP_SERVER_PORT is undefined');
}

server.listen(Number(HTTP_SERVER_PORT));

console.log(`HTTP Server started on PORT:${HTTP_SERVER_PORT}`);

const shutdown = () => {
  console.log('Shutting down server');

  server.server.close(() => process.exit(0));
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
