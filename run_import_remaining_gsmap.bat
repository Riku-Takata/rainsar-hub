@echo off
echo ========================================================
echo Importing GSMaP data from 2015 onwards...
echo (This process may take several hours depending on PC specs)
echo ========================================================

docker run --name gsmap-import-others --rm --network container:rainsarhub-db -e DB_HOST=localhost -e DB_PORT=3306 -e DB_USER=rainsar -e DB_PASSWORD=rainsar_pw -e DB_NAME=rainsar_hub -v "E:\gsmap-data:/data/gsmap" -v "%~dp0backend:/app" -w /app rainsar-hub-backend python -m scripts.import_gsmap_points --root /data/gsmap --start-year 2015 --min-gauge-mm-h 7.0 --workers 6

echo ========================================================
echo Import finished or interrupted.
echo ========================================================
