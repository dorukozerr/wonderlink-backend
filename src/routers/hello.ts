import { z } from 'zod';
import { router, publicProcedure } from '../trpc';

export const helloRouter = router({
  greeting: publicProcedure
    .input(
      z.object({
        name: z.string().optional()
      })
    )
    .query(async ({ input }) => ({
      greeting: `Hello ${input?.name ?? 'world'}!`
    }))
});
