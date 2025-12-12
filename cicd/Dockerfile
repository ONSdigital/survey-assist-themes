# Stage 1: Build stage
FROM python:3.12-slim AS builder

ENV POETRY_VERSION=2.1.1 \
    POETRY_HOME="/opt/poetry" \
    PATH="$POETRY_HOME/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        build-essential \
        git \
        && curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN /opt/poetry/bin/poetry config virtualenvs.create false && \
    /opt/poetry/bin/poetry install --no-root --only main && \
    rm -rf ~/.cache /root/.cache /tmp/*

COPY src/ ./src/
COPY cicd/themes-job.py ./themes-job.py
ENV PYTHONPATH="/app/src:${PYTHONPATH}"

# Stage 2: Final runtime image
FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /app /app
COPY --from=builder /opt/poetry /opt/poetry
COPY --from=builder /usr/local/lib/python3.12 /usr/local/lib/python3.12
COPY --from=builder /usr/local/bin /usr/local/bin
ENV PATH="/opt/poetry/bin:$PATH" \
    PYTHONPATH="/app/src:${PYTHONPATH}"

# Create non-root user
RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid appuser --shell /bin/bash --create-home appuser && \
    chown -R appuser:appuser /app

USER appuser

# set the default executable for a container 
ENTRYPOINT [ "python" ]

# Define the command to run your Python script
CMD ["themes-job.py"]
