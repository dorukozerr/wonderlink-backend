import { initTRPC } from '@trpc/server';
import { CreateHTTPContextOptions } from '@trpc/server/adapters/standalone';

export const createContext = async (opts: CreateHTTPContextOptions) => ({
  req: opts.req,
  res: opts.res
});

export type Context = Awaited<ReturnType<typeof createContext>>;

const t = initTRPC.context<Context>().create();

export const router = t.router;
export const publicProcedure = t.procedure;
