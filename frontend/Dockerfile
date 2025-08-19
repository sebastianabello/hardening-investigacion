# Etapa de build
FROM node:20-alpine AS builder
WORKDIR /app

# Copiamos manifiestos primero (si hay lock, se copia por el wildcard)
COPY package.json package-lock.json* ./

# Si existe package-lock.json → npm ci, si no → npm install
RUN if [ -f package-lock.json ]; then \
  npm ci --no-audit --no-fund; \
  else \
  echo "package-lock.json no encontrado. Usando npm install."; \
  npm install --no-audit --no-fund; \
  fi

# Ahora copiamos el resto del código
COPY . .

# VITE_API para el build
ARG VITE_API=/api
ENV VITE_API=$VITE_API

RUN npm run build

# Etapa de runtime (Nginx)
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
