# No build step: tsx runs the TypeScript source directly, same as PA2's
# services. See ../docs/adr-004.md for why this project prefers tsx over a
# tsc-then-node compile step for its long-running consumer/producer daemons.
FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install
RUN ln -s /app/node_modules /node_modules
COPY src ./src

CMD ["npx", "tsx", "src/server.ts"]
