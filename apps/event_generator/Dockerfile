FROM python:3.11-slim

WORKDIR /app

COPY apps/event_generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/event_generator /app/event_generator
COPY reference-data /app/reference-data

ENV PYTHONPATH=/app

CMD ["python", "-m", "event_generator.main", "--bootstrap-servers", "kafka:29092", "--flows", "100", "--interval-ms", "500"]
