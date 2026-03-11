FROM python:3.11-slim

# Installer Stockfish via apt (disponible à /usr/games/stockfish)
RUN apt-get update && \
    apt-get install -y stockfish && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code (sans .env — les secrets viennent des variables Railway)
COPY server.py .
COPY puzzles_lichess.csv* ./

# PORT est fourni dynamiquement par Railway
CMD uvicorn server:app --host 0.0.0.0 --port ${PORT:-8000}
