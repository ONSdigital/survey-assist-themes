FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends

WORKDIR /app

COPY requirements.txt .
COPY themes-job.py .
RUN pip install -r requirements.txt

# Create non-root user
RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid appuser --shell /bin/bash --create-home appuser && \
    chown -R appuser:appuser /app

USER appuser

# set the default executable for a container 
ENTRYPOINT [ "python" ]

# Define the command to run your Python script
CMD ["themes-job.py"]