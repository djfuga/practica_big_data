#!/bin/sh
# =============================================================
# Bootstrap de MinIO: crea bucket Lakehouse, usuario de app
# Se ejecuta automaticamente desde docker-compose
# Es idempotente: se puede ejecutar N veces sin romper nada
# =============================================================

set -e

echo "==> Configurando alias 'local' contra MinIO..."
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

echo "==> Comprobando conexion..."
mc admin info local

echo "==> Creando bucket '$MINIO_LAKEHOUSE_BUCKET' (si no existe)..."
mc mb --ignore-existing local/"$MINIO_LAKEHOUSE_BUCKET"

echo "==> Creando estructura de directorios del Lakehouse..."
# Iceberg organiza por: warehouse/<database>/<table>/
mc mb --ignore-existing local/"$MINIO_LAKEHOUSE_BUCKET"/warehouse
mc mb --ignore-existing local/"$MINIO_LAKEHOUSE_BUCKET"/raw
mc mb --ignore-existing local/"$MINIO_LAKEHOUSE_BUCKET"/models

echo "==> Creando usuario de aplicacion '$MINIO_APP_USER'..."
# 'mc admin user add' es idempotente desde RELEASE.2024-x
mc admin user add local "$MINIO_APP_USER" "$MINIO_APP_PASSWORD" || true

echo "==> Asignando politica 'readwrite' al usuario de app..."
mc admin policy attach local readwrite --user "$MINIO_APP_USER" || true

echo "==> Listando buckets:"
mc ls local

echo ""
echo "===================================================="
echo "  MinIO bootstrap completado"
echo "  - Bucket principal: $MINIO_LAKEHOUSE_BUCKET"
echo "  - Usuario app:      $MINIO_APP_USER"
echo "  - API S3:           http://minio:9000 (interno)"
echo "                      http://localhost:9000 (host)"
echo "  - Console UI:       http://localhost:9001"
echo "===================================================="
