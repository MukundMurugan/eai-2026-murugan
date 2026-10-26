FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install
RUN ln -s /app/node_modules /node_modules

COPY src ./src

CMD ["npx", "tsx", "src/server.ts"]
