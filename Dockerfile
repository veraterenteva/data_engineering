# 1. Базовый образ
FROM python:3.11-slim

# 2. Рабочая директория
WORKDIR /app

# 3. Копируем проект
COPY . .

# 4. Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# 5. Настройки среды
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/src:${PATH}"

# 6. Команда по умолчанию
CMD ["python", "-m", "etl.main", "--info"]
