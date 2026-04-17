FROM python:3.11-slim

WORKDIR /app

# Copy requirements first so pip install is cached as its own layer.
# This layer only re-runs when requirements.txt changes, not on every code push.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

EXPOSE 8080

CMD gunicorn main:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120
