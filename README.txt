BACKEND — SERVEUR API FASTAPI
==============================

Serveur Python responsable de toute la logique métier : moteur d'échecs,
analyse des parties, import Chess.com, coach IA et persistance des données.

FICHIERS
--------
server.py           Point d'entrée principal du serveur FastAPI.
                    Contient TOUTES les routes API, la logique Stockfish,
                    la connexion MongoDB, l'import Chess.com et le bot coach.

requirements.txt    Dépendances Python du projet (~127 packages).

RÔLE DE server.py
------------------
  Routes API (préfixe /api) :
    - POST /api/make-move         Jouer un coup (validation + réponse du bot)
    - POST /api/analyze-game      Analyser une partie complète (Stockfish)
    - POST /api/import-chesscom   Importer les parties depuis Chess.com
    - POST /api/player-profile    Générer le profil Stockfish d'un joueur
    - GET  /api/puzzle            Récupérer un puzzle aléatoire (Lichess DB)
    - POST /api/coach-message     Générer un message du coach selon le contexte

  Intégrations :
    - Stockfish : analyse coup par coup (CPL, accuracy, classification)
    - MongoDB   : stockage des parties, profils, stats (via Motor async)
    - Chess.com : import des 50 dernières parties (3 derniers mois)
    - OpenAI    : génération de messages coach narratifs

DÉPENDANCES CLÉS (requirements.txt)
-------------------------------------
  fastapi / uvicorn     Serveur web async
  motor / pymongo       Driver MongoDB asynchrone
  python-chess          Logique échecs + interface Stockfish
  openai                Messages coach IA
  pandas / numpy        Analyse statistique des parties
  pydantic              Validation des données
  boto3                 (AWS S3, optionnel)
  stripe                (Paiements, optionnel)

LANCER LE SERVEUR
-----------------
  cd backend
  pip install -r requirements.txt
  uvicorn server:app --reload --port 8000
