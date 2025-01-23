import { router } from '../trpc';
import { analyticsRouter } from './analytics';
import { filterFieldsRouter } from './filter-fields';

export const appRouter = router({
  analytics: analyticsRouter,
  'filter-fields': filterFieldsRouter
});

export type AppRouter = typeof appRouter;
