# Remove the problematic single quotes
Start-Process -FilePath ".\dfsapi.exe" -ArgumentList "--node 0 --http localhost:8081 --cluster 1,localhost:3030,localhost:8081;2,localhost:3031,localhost:8082;3,localhost:3032,localhost:8083"
Start-Process -FilePath ".\dfsapi.exe" -ArgumentList "--node 1 --http localhost:8082 --cluster 1,localhost:3030,localhost:8081;2,localhost:3031,localhost:8082;3,localhost:3032,localhost:8083"
Start-Process -FilePath ".\dfsapi.exe" -ArgumentList "--node 2 --http localhost:8083 --cluster 1,localhost:3030,localhost:8081;2,localhost:3031,localhost:8082;3,localhost:3032,localhost:8083"

Write-Host "All nodes started!"
Write-Host "HTTP APIs available on:"
Write-Host "  Node 1: http://localhost:8081"
Write-Host "  Node 2: http://localhost:8082" 
Write-Host "  Node 3: http://localhost:8083"
Write-Host ""
Write-Host "Test with: curl -X POST --data-binary @file.txt http://localhost:8081/upload/test.txt"