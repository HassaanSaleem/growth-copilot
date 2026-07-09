# Runtime image for the HTTP API — no dev dependencies, no test suite.
#
#   docker build -t growth-copilot .
#   docker run --rm -p 8000:8000 -v ./data:/app/data growth-copilot
#
# The warehouse and checkpoints live under ./data inside the container;
# mount a volume there so seeded data survives the container.

FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .

EXPOSE 8000

# `growth-copilot serve` runs uvicorn against the api.py app factory —
# the same entrypoint documented for local use.
CMD ["growth-copilot", "serve", "--host", "0.0.0.0", "--port", "8000"]
