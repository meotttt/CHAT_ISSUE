    # Используйте стабильную версию Python. 3.12 - хороший выбор.
    # 'slim-bookworm' - это облегченный образ Debian Bookworm, подходит для продакшена.
    FROM python:3.12-slim-bookworm

    # Устанавливаем необходимые системные зависимости для psycopg2 и других возможных инструментов.
    # 'libpq-dev' предоставляет заголовочные файлы для PostgreSQL, нужные psycopg2.
    # 'build-essential' и 'gcc' предоставляют компиляторы и утилиты сборки.
    RUN apt-get update && apt-get install -y \
        build-essential \
        libpq-dev \
        git \
        gcc \
        # Добавьте другие системные зависимости, если ваш проект их требует
        # Например: vim, nano, locales и т.д.
        && rm -rf /var/lib/apt/lists/*

    # Устанавливаем рабочую директорию внутри контейнера
    WORKDIR /app

    # Копируем файл зависимостей и устанавливаем их.
    # Делаем это первым, чтобы Docker мог кэшировать этот слой, если requirements.txt не меняется.
    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt

    # Копируем остальной код вашего приложения в контейнер
    COPY . .

    # Устанавливаем переменные окружения, если это необходимо
    # PYTHONUNBUFFERED=1 гарантирует, что вывод Python будет немедленно отправляться в логи
    ENV PYTHONUNBUFFERED=1

    # Команда, которая будет выполняться при запуске контейнера
    # Убедитесь, что 'chat.py' - это основной файл, который запускает ваше приложение.
    CMD ["python3", "chat.py"]
