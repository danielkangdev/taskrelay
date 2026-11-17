import { benchHandler } from './bench.js';
import { reportHandler } from './report.js';
import type { HandlerRegistry } from './types.js';

export const defaultHandlers: HandlerRegistry = {
  report: reportHandler,
  bench: benchHandler,
};
