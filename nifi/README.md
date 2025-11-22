# NIFI
1) run `sudo chown -R 1000:1000 nifi-registry-data`
2) run `docker compose up`
3) login using `admin` login and `admin123456!` password

## Importing Flows from NiFi Registry
1. Open NiFi UI: https://localhost:8443/nifi
2. Global menu (top right) -> Controller Settings -> Registry Clients -> Add.
   - URL: http://nifi-registry:18080 (inside docker network) or http://localhost:18080 externally.
3. Ensure buckets exist (create via Registry UI if needed).
4. In NiFi canvas: + (add) -> "Import from Registry" (or right-click canvas) -> select Bucket, Flow, Version -> Import.
5. To version a process group: Right-click group -> "Version" -> "Start version control" -> choose bucket & flow name.

## Key Endpoints
NiFi UI: https://localhost:8443/nifi  
NiFi Registry API: http://localhost:18080/nifi-registry-api