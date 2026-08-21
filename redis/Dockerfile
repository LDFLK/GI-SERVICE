FROM redis:7-alpine

EXPOSE 6379

CMD ["redis-server", "--bind", "0.0.0.0", "--port", "6379", "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"]
