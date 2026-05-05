FROM node:24-alpine AS deps

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

FROM node:24-alpine AS builder

WORKDIR /app

ARG NEXT_PUBLIC_PRODUCT_SYNC_ENDPOINT=""
ARG NEXT_PUBLIC_PRODUCT_SYNC_PASSWORD=""
ENV NEXT_PUBLIC_PRODUCT_SYNC_ENDPOINT=$NEXT_PUBLIC_PRODUCT_SYNC_ENDPOINT
ENV NEXT_PUBLIC_PRODUCT_SYNC_PASSWORD=$NEXT_PUBLIC_PRODUCT_SYNC_PASSWORD

COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:24-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=8000
ENV HOSTNAME=0.0.0.0

COPY --from=builder /app/public ./public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static

EXPOSE 8000

CMD ["node", "server.js"]
