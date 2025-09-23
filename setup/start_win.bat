@echo off
REM Build and run the Docker container
docker-compose down --remove-orphans
docker-compose up -d --build

REM Display SSH connection information
echo Docker container is running. You can connect via SSH with:
echo "ssh root@localhost -p 2222"

REM Get the container ID or name dynamically using the service name
set service_name=sageflow
FOR /F "tokens=*" %%i IN ('docker-compose ps -q %service_name%') DO set container_name=%%i

REM Wait for the container to start
timeout /t 5

REM Automatically attach to the container's bash shell
docker exec -it %container_name% /bin/bash