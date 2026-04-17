FROM python:3.13-slim

WORKDIR /app

# Copy requirements first so pip install is cached as its own layer.
# This layer only re-runs when requirements.txt changes, not on every code push.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the operator application code
COPY . .

EXPOSE 8080

CMD ["sh", "-c", "gunicorn 'app:create_app()' --bind 0.0.0.0:$PORT --workers 2 --timeout 120"]
