FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml .

# Install dependencies into the system Python (no venv inside container)
RUN uv pip install --system --no-cache -r pyproject.toml

# Copy source code
COPY src/ ./src/

ENV PYTHONPATH=/app/src

CMD ["python", "src/main.py"]
