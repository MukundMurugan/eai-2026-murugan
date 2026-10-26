import { defineConfig } from "vitest/config";

// Public tests live in ./public and ship with the assignment. WP-13's brief
// does not add hidden PA5 test cases (unlike PA5's siblings) — all seven of
// the original lab's tests are public here — but the include pattern still
// picks up ./hidden if a grading box ever drops one there, at no cost when
// it doesn't exist.
export default defineConfig({
  test: {
    include: ["public/**/*.test.ts", "hidden/**/*.test.ts"],
    reporters: ["verbose"],
    // The suite drives real HTTP/AMQP traffic through Docker containers,
    // restarts services mid-run, and waits out 1s TTL retry cycles more than
    // once — the original jest.setTimeout(120000) carries over unchanged.
    testTimeout: 120_000,
    hookTimeout: 120_000,
    // Tests share one Docker Compose stack (stopping/starting service
    // containers, purging shared queues) — running test files concurrently
    // would race on that shared state. There is only one test file here,
    // but this also disables in-file concurrency, matching the original
    // jest --runInBand.
    fileParallelism: false,
  },
});
