@echo off
chcp 65001 >nul
echo ============================================
echo 🚀 Запуск системы управления активами V11
echo ============================================
echo.

REM Проверка наличия Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Ошибка: Python не установлен или не добавлен в PATH
    pause
    exit /b 1
)

REM Проверка версии Python
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo ✅ Python версия: %PYTHON_VERSION%

REM Проверка и установка зависимостей
echo 📦 Проверка зависимостей...
pip install -r requirements.txt

REM Создание необходимых директорий
if not exist "data" mkdir data
if not exist "static\uploads" mkdir static\uploads
if not exist "static\exports" mkdir static\exports
if not exist "logs" mkdir logs
if not exist "backups" mkdir backups

REM Проверка наличия .env
if not exist ".env" (
    echo.
    echo ⚠️  Файл .env не найден!
    if exist ".env.example" (
        copy ".env.example" ".env" >nul
        echo 📋 Создан .env из шаблона .env.example
        echo.
        echo ❗ ВАЖНО: Откройте .env и замените SECRET_KEY на уникальный ключ.
        echo    Генерация ключа: python -c "import secrets; print(secrets.token_hex(32))"
        echo.
    ) else (
        echo ❌ Файл .env.example тоже не найден. Создайте .env вручную.
        pause
        exit /b 1
    )
)

REM Проверка вендорных библиотек
if not exist "static\vendor\bootstrap\bootstrap.min.css" (
    echo.
    echo ⚠️  Папка static\vendor не найдена или неполная.
    echo    Скопируйте библиотеки вручную или переустановите систему.
)

REM Проверка наличия базы данных
if not exist "data\assets.db" (
    echo ⚠️  База данных не найдена!
    echo 📝 Создание новой базы данных...
    
    REM Создаем Python скрипт для инициализации базы
    echo import sys > init_db.py
    echo sys.path.append('.') >> init_db.py
    echo from database import Database >> init_db.py
    echo db = Database('data/assets.db') >> init_db.py
    echo print('✅ База данных создана успешно') >> init_db.py
    
    python init_db.py
    
    if errorlevel 1 (
        echo ❌ Ошибка при создании базы данных
        del init_db.py
        pause
        exit /b 1
    )
    
    del init_db.py
)

echo.
echo ============================================
echo ✅ Все проверки пройдены успешно!
echo.
echo 🌐 Запуск веб-приложения...
echo 📍 Адрес: http://localhost:5000
echo 👤 Демо доступ: нету
echo ============================================
echo.

REM Запуск приложения в фоновом режиме
start /B python app.py

REM Ждем немного, чтобы сервер успел запуститься
timeout /t 3 /nobreak >nul

REM Открываем браузер
echo 📂 Открываю браузер...
start "" "http://localhost:5000"

echo Нажмите Ctrl+C для остановки сервера
echo.

REM Ждем завершения процесса Python
waitfor /T 9999999 SomethingThatWillNeverHappen 2>nul

if errorlevel 1 (
    echo.
    echo ❌ Ошибка при запуске приложения
    pause
    exit /b 1
)

pause