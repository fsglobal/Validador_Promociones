@echo off
chcp 65001 >nul

title Validador de Promociones - WEB

echo ========================================================
echo        INICIANDO SERVIDOR WEB DEL VALIDADOR
echo ========================================================

REM --------------------------------------------------------
REM 1. BUSCAR PYTHON
REM --------------------------------------------------------

for %%p in (
    "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
    "%ProgramFiles%\Python314\python.exe"
    "%ProgramFiles(x86)%\Python314\python.exe"
    "python.exe"
) do (
    if exist %%p (
        set PYTHON=%%p
        goto py_found
    )
)

echo ❌ Python no está instalado o no se pudo detectar.
echo Por favor instale Python 3.10+ manualmente.
pause
exit /b

:py_found
echo ✔ Python encontrado en:
echo %PYTHON%
echo.

REM --------------------------------------------------------
REM 2. VERIFICAR PIP
REM --------------------------------------------------------

echo Verificando pip...
%PYTHON% -m pip --version >nul 2>&1
if errorlevel 1 (
    echo ❗ pip no está instalado. Intentando instalar...
    %PYTHON% -m ensurepip --default-pip
)

echo ✔ pip disponible.
echo.

REM --------------------------------------------------------
REM 3. INSTALAR DEPENDENCIAS AUTOMÁTICAMENTE
REM --------------------------------------------------------

echo Instalando dependencias necesarias...

set PACKAGES=openpyxl pandas flask colorama

for %%P in (%PACKAGES%) do (
    echo ➜ Verificando %%P...
    %PYTHON% -m pip show %%P >nul 2>&1
    if errorlevel 1 (
        echo     Instalando %%P...
        %PYTHON% -m pip install %%P --quiet
        if errorlevel 1 (
            echo ❌ ERROR instalando %%P
            pause
            exit /b
        )
        echo     ✔ %%P instalado.
    ) else (
        echo     ✔ %%P ya instalado.
    )
)

echo.
echo ✔ Todas las dependencias están listas.
echo.

REM --------------------------------------------------------
REM 4. ABRIR AUTOMÁTICAMENTE EL NAVEGADOR
REM --------------------------------------------------------

echo Abriendo navegador en: http://127.0.0.1:5000
start "" http://127.0.0.1:5000

REM --------------------------------------------------------
REM 5. INICIAR SERVIDOR FLASK
REM --------------------------------------------------------

echo Ejecutando servidor Flask...
echo (Deje esta ventana abierta)
echo.

cd /d "%~dp0web"
echo ------------------------------------------------------ >> "%~dp0\logs\bat_debug.log"
echo %DATE% %TIME% - Iniciando ejecución >> "%~dp0\logs\bat_debug.log"
echo Archivos Excel actuales: >> "%~dp0\logs\bat_debug.log"
dir "%~dp0Excel" >> "%~dp0\logs\bat_debug.log"
echo Archivos Export actuales: >> "%~dp0\logs\bat_debug.log"
dir "%~dp0Export" >> "%~dp0\logs\bat_debug.log"
echo ------------------------------------------------------ >> "%~dp0\logs\bat_debug.log"

%PYTHON% app.py

echo.
echo ========================================================
echo        SERVIDOR FINALIZADO
echo ========================================================
pause
exit /b
