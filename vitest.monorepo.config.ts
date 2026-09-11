import { defineConfig } from 'vitest/config';
import { cloudflareTest } from '@cloudflare/vitest-pool-workers';

/**
 * The cross-surface drift suite, which only runs inside the QBSheet monorepo.
 *
 * `test/` is the relay's own suite and stays inside this directory's boundary, because this
 * directory is copied by itself into an empty repository by "Deploy to Cloudflare" (see
 * `README.md`). `test-monorepo/` is the other half of the bargain the README describes: the relay
 * reimplements the #770 contract rather than importing it, and what stops the two copies drifting
 * is a suite that drives the real Worker against the canonical fixtures in `tests/fixtures/` and
 * the real scorer and Director clients in `src/`.
 *
 * Those imports reach outside this directory on purpose, and that is exactly why they are not in
 * `test/`: `npm test` and `npm run typecheck` have to pass in the copied-away directory, where
 * nothing above it exists. Splitting the suites is what lets both facts be true at once. The
 * monorepo runs this file through `npm run test:monorepo`.
 */
export default defineConfig({
  plugins: [
    cloudflareTest({
      singleWorker: true,
      wrangler: { configPath: './wrangler.jsonc' },
      miniflare: {
        bindings: {
          RELAY_SETUP_TOKEN: 'test-setup-token',
          RELAY_ALLOWED_ORIGINS: 'https://qbsheet.com,https://scorer.example,https://director.example',
        },
      },
    }),
  ],
  test: {
    include: ['test-monorepo/**/*.test.ts'],
  },
});
