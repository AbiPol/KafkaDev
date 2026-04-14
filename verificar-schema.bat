@echo off
echo === Subject registrados ===
docker exec schema-registry-app curl -s http://schema-registry:8081/subjects

echo.
echo === Schema compraspValue v1 ===
docker exec schema-registry-app curl -s http://schema-registry:8081/subjects/compras-value/versions/1

echo.
echo === Compatibilidad global ===
docker exec schema-registry-app curl -s http://schema-registry:8081/config