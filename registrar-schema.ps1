$schema = @'
{"type":"record","name":"Compra","namespace":"com.tienda.eventos","fields":[{"name":"userId","type":"string"},{"name":"username","type":"string"},{"name":"email","type":"string"},{"name":"monto","type":"double"},{"name":"timestamp","type":{"type":"long","logicalType":"timestamp-millis"}}]}
'@
$body = @{schema=$schema} | ConvertTo-Json
docker exec schema-registry-app curl -X POST -H "Content-Type: application/json" -d $body http://schema-registry:8081/subjects/compras-value/versions