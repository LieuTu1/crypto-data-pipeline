@echo off
setlocal
cd /d "%~dp0"
set PORT=7213
echo Starting static server at %cd% on port %PORT%.
python -m http.server %PORT%
