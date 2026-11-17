import { defineConfig } from 'vitest/config';

// Every test file talks to one real PostgreSQL database and truncates tables
// between tests, so files must not run in parallel.
export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    globalSetup: ['tests/global-setup.ts'],
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
