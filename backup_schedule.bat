@echo off
chcp 65001 >nul
REM =========================================================
REM Скрипт настройки автоматического резервного копирования
REM Создаёт задачу в Windows Task Scheduler
REM Запуск ОДИН РАЗ от имени администратора
REM =========================================================

echo ============================================
echo Настройка автоматического резервного копирования
echo ============================================
echo.

REM Определяем полный путь к Python и скрипту
for /f "delims=" %%i in ('python -c "import sys; print(sys.executable)"') do set PYTHON_PATH=%%i
set SCRIPT_PATH=%~dp0backup_db.py
set WORKING_DIR=%~dp0

echo Python: %PYTHON_PATH%
echo Скрипт: %SCRIPT_PATH%
echo.

REM Удаляем старую задачу если есть
schtasks /delete /tn "InventoryBotBackup" /f >nul 2>&1

REM Создаём новую задачу: каждый день в 02:00
schtasks /create ^
    /tn "InventoryBotBackup" ^
    /tr "\"%PYTHON_PATH%\" \"%SCRIPT_PATH%\"" ^
    /sc daily ^
    /st 02:00 ^
    /ru "%USERNAME%" ^
    /rl highest ^
    /f

if errorlevel 1 (
    echo.
    echo Ошибка создания задачи. Запустите скрипт от имени администратора.
    pause
    exit /b 1
)

echo.
echo Задача успешно создана!
echo Резервные копии будут создаваться ежедневно в 02:00.
echo Хранятся в папке: %WORKING_DIR%backups\
echo.
echo Для ручного запуска резервного копирования:
echo   python backup_db.py
echo.
echo Для просмотра списка копий:
echo   python backup_db.py --list
echo.
pause
