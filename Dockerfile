FROM python:3.9-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта в контейнер
COPY . /app

# Устанавливаем зависимости
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Устанавливаем переменные окружения
# Примечание: В реальном контейнере секреты лучше передавать через docker run --env-file или volumes
ENV GOOGLE_API_CREDENTIALS_PATH=/app/credentials.json

# Открываем порт (если нужен веб-сервер Flask)
EXPOSE 5000

# Запускаем бота
CMD ["python", "bot.py"]
