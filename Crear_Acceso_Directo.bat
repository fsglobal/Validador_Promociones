@echo off
chcp 65001 >nul

echo ========================================================
echo      CREANDO ACCESO DIRECTO DEL VALIDADOR
echo ========================================================

REM Detectar el escritorio real del usuario
for /f "usebackq tokens=*" %%d in (`powershell -NoProfile -Command ^
    "[Environment]::GetFolderPath('Desktop')"`) do set DESKTOP_PATH=%%d

echo Escritorio detectado: %DESKTOP_PATH%
echo.

REM Archivo destino
set SHORTCUT="%DESKTOP_PATH%\Validador de Promociones.lnk"

REM Ruta del ejecutor
set TARGET="%~dp0Ejecutar_Web_Validador.bat"

REM Icono opcional
set ICON="%~dp0icono.ico"

REM Crear script temporal VBS
set VBS=%TEMP%\shortcut_creator.vbs
del "%VBS%" >nul 2>&1

echo Set oWS = WScript.CreateObject("WScript.Shell") >> "%VBS%"
echo sLinkFile = %SHORTCUT% >> "%VBS%"
echo Set oLink = oWS.CreateShortcut(sLinkFile) >> "%VBS%"
echo oLink.TargetPath = %TARGET% >> "%VBS%"
echo oLink.IconLocation = %ICON% >> "%VBS%"
echo oLink.WorkingDirectory = "%~dp0" >> "%VBS%"
echo oLink.Save >> "%VBS%"

cscript //nologo "%VBS%" >nul 2>&1

IF EXIST %SHORTCUT% (
    echo ✔ Acceso directo creado correctamente!
) ELSE (
    echo ❌ Error: No se pudo crear el acceso directo.
    echo Posibles causas:
    echo - Escritorio bajo OneDrive bloqueado
    echo - Antivirus bloqueando archivos .lnk
    echo - Falta de permisos sobre el escritorio
)

echo.
pause
exit /b
