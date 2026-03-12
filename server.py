from fastapi import FastAPI, APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from dotenv import load_dotenv
from pathlib import Path as _Path
load_dotenv(_Path(__file__).parent / '.env')
from starlette.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime
import csv
import math
import asyncio
import threading
import random
import re
import json
from enum import Enum
import requests
import io
import zstandard as zstd
import chess
import chess.pgn
import chess.engine
from openai import OpenAI
import hashlib
import redis.asyncio as aioredis

STOCKFISH_PATH = os.getenv("STOCKFISH_PATH", "/usr/games/stockfish")
if not os.path.exists(STOCKFISH_PATH):
    fallback_path = "/usr/bin/stockfish"
    if os.path.exists(fallback_path):
        STOCKFISH_PATH = fallback_path
STOCKFISH_TIME_LIMIT = float(os.getenv("STOCKFISH_TIME_LIMIT", "0.08"))
STOCKFISH_DEPTH = int(os.getenv("STOCKFISH_DEPTH", "12"))
STOCKFISH_TIME_LIMIT_IMPORT = float(os.getenv("STOCKFISH_TIME_LIMIT_IMPORT", "0.02"))
STOCKFISH_DEPTH_IMPORT = int(os.getenv("STOCKFISH_DEPTH_IMPORT", "8"))

PUZZLE_DB_URL = os.getenv("LICHESS_PUZZLES_URL", "https://database.lichess.org/lichess_db_puzzle.csv.zst")
PUZZLE_CACHE_SIZE = int(os.getenv("PUZZLE_CACHE_SIZE", "1500"))
PUZZLE_RATING_MIN = int(os.getenv("PUZZLE_RATING_MIN", "1500"))
PUZZLE_RATING_MAX = int(os.getenv("PUZZLE_RATING_MAX", "1700"))

PUZZLE_GROUPS = [
    ("TACTIQUES", ["fork", "pin", "skewer", "discoveredAttack", "doubleAttack", "attackOnF7", "deflection"]),
    ("MATS", ["mateIn1", "mateIn2", "mateIn3", "backRankMate", "smotheredMate"]),
    ("CALCUL", ["sacrifice", "intermezzo", "quietMove", "attraction", "clearance"]),
    ("FINALES", ["endgame", "rookEndgame", "pawnEndgame", "queenEndgame", "bishopEndgame", "knightEndgame"]),
    ("DEFENSE", ["defensiveMove", "perpetualCheck", "stalemate", "resource"]),
    ("POSITIONNEL", ["zugzwang", "quietMove", "positional"]),
]

STOCKFISH_POOL_SIZE = int(os.getenv("STOCKFISH_POOL_SIZE", "3"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Premium feature flag — set PREMIUM_ENABLED=true in Railway to activate
PREMIUM_ENABLED = os.getenv("PREMIUM_ENABLED", "false").lower() == "true"
FREE_MAX_ANALYSES = 3
FREE_MAX_GAMES = 5
PREMIUM_MAX_GAMES = 20
# Bypass list for dev/testing — comma-separated user_ids exempt from limits
# Example: BYPASS_PREMIUM_USER_IDS=user_123abc,user_456def
_bypass_raw = os.getenv("BYPASS_PREMIUM_USER_IDS", "")
BYPASS_PREMIUM_USER_IDS: set = {uid.strip() for uid in _bypass_raw.split(",") if uid.strip()}
REDIS_TTL_EVAL = 3600    # 1h pour les évaluations de position
REDIS_TTL_PROFILE = 86400  # 24h pour les profils joueurs

WEAKNESS_TO_THEMES = {
    "discipline":  ["fork", "pin", "skewer", "doubleAttack", "discoveredAttack", "hangingPiece"],
    "king_safety": ["kingsideAttack", "backRankMate", "mateIn1", "mateIn2", "attackingF2F7"],
    "tactics":     ["fork", "pin", "deflection", "sacrifice", "intermezzo", "attraction", "clearance"],
    "structure":   ["endgame", "rookEndgame", "pawnEndgame", "zugzwang"],
    "conversion":  ["endgame", "queenEndgame", "rookEndgame", "advantage"],
}

WEAKNESS_LABELS = {
    "discipline":  "DISCIPLINE",
    "king_safety": "SÉCURITÉ ROI",
    "tactics":     "TACTIQUES",
    "structure":   "STRUCTURE",
    "conversion":  "CONVERSION",
}

PLAYER_ARCHETYPES = [
    {
        "id": "stratege_froid",
        "name": "Le Stratège Froid",
        "description": "Joueur positionnel et méthodique. Chaque coup est une pièce d'un plan à long terme.",
        "color": "#81b64c",
        "weights": {"precision": 0.25, "strategy": 0.35, "defense": 0.20, "conversion": 0.10, "tactics": 0.10, "aggression": 0.0, "chaos": 0.0, "exploitation": 0.0},
    },
    {
        "id": "predateur_tactique",
        "name": "Le Prédateur Tactique",
        "description": "Attaque sans répit. Son jeu explosif désarçonne les défenses adverses.",
        "color": "#c0392b",
        "weights": {"aggression": 0.30, "tactics": 0.30, "chaos": 0.20, "exploitation": 0.20, "precision": 0.0, "strategy": 0.0, "defense": 0.0, "conversion": 0.0},
    },
    {
        "id": "architecte_silencieux",
        "name": "L'Architecte Silencieux",
        "description": "Bâtisseur de positions. Il prépare le terrain avant de frapper.",
        "color": "#9dc86a",
        "weights": {"strategy": 0.30, "tactics": 0.25, "precision": 0.20, "conversion": 0.15, "defense": 0.10, "aggression": 0.0, "chaos": 0.0, "exploitation": 0.0},
    },
    {
        "id": "berserker",
        "name": "Le Berserker",
        "description": "Furie pure et puissance brute. Il écrase l'adversaire sous un déluge de coups violents.",
        "color": "#a0522d",
        "weights": {"exploitation": 0.35, "precision": 0.30, "tactics": 0.20, "conversion": 0.15, "aggression": 0.0, "strategy": 0.0, "defense": 0.0, "chaos": 0.0},
    },
    {
        "id": "illusionniste",
        "name": "L'Illusionniste",
        "description": "Crée des positions obscures et complexes. Son adversaire ne sait jamais où il va.",
        "color": "#f0ad4e",
        "weights": {"chaos": 0.35, "tactics": 0.30, "aggression": 0.20, "exploitation": 0.15, "precision": 0.0, "strategy": 0.0, "defense": 0.0, "conversion": 0.0},
    },
    {
        "id": "mur_acier",
        "name": "Le Mur d'Acier",
        "description": "Forteresse imprenable. Il défend sans trembler et contre-attaque au bon moment.",
        "color": "#8a9ba8",
        "weights": {"defense": 0.40, "precision": 0.20, "strategy": 0.20, "conversion": 0.20, "aggression": 0.0, "tactics": 0.0, "chaos": 0.0, "exploitation": 0.0},
    },
    {
        "id": "chasseur_opportuniste",
        "name": "Le Chasseur Opportuniste",
        "description": "Flair tactique redoutable. Il saisit chaque occasion sans prévenir.",
        "color": "#d4a843",
        "weights": {"tactics": 0.30, "aggression": 0.25, "exploitation": 0.25, "chaos": 0.20, "precision": 0.0, "strategy": 0.0, "defense": 0.0, "conversion": 0.0},
    },
    {
        "id": "chirurgien",
        "name": "Le Chirurgien",
        "description": "Précision absolue, zéro gâchis. Il convertit ses avantages avec une rigueur clinique.",
        "color": "#81b64c",
        "weights": {"precision": 0.35, "conversion": 0.35, "tactics": 0.15, "defense": 0.15, "aggression": 0.0, "strategy": 0.0, "chaos": 0.0, "exploitation": 0.0},
    },
    {
        "id": "chaos_maitrise",
        "name": "Le Chaos Maîtrisé",
        "description": "Il crée le désordre mais reste maître du jeu. L'imprévisible sous contrôle.",
        "color": "#c8872a",
        "weights": {"chaos": 0.30, "tactics": 0.25, "aggression": 0.25, "strategy": 0.20, "precision": 0.0, "defense": 0.0, "conversion": 0.0, "exploitation": 0.0},
    },
    {
        "id": "calculateur",
        "name": "Le Calculateur",
        "description": "Profondeur de calcul supérieure. Il voit plusieurs coups d'avance et ne se trompe pas.",
        "color": "#6a9a3e",
        "weights": {"tactics": 0.35, "precision": 0.30, "strategy": 0.20, "conversion": 0.15, "aggression": 0.0, "defense": 0.0, "chaos": 0.0, "exploitation": 0.0},
    },
    {
        "id": "assassin_roi",
        "name": "L'Assassin du Roi",
        "description": "Il flaire le roi adverse. Une fois sur la piste, rien ne l'arrête.",
        "color": "#c0392b",
        "weights": {"aggression": 0.40, "chaos": 0.25, "tactics": 0.20, "exploitation": 0.15, "precision": 0.0, "strategy": 0.0, "defense": 0.0, "conversion": 0.0},
    },
    {
        "id": "executeur",
        "name": "L'Exécuteur",
        "description": "Inexorable en finale. Quand il a l'avantage, l'issue est inévitable.",
        "color": "#7a6a8a",
        "weights": {"conversion": 0.35, "precision": 0.30, "defense": 0.20, "strategy": 0.15, "aggression": 0.0, "tactics": 0.0, "chaos": 0.0, "exploitation": 0.0},
    },
    {
        "id": "reine_nocturne",
        "name": "La Reine Nocturne",
        "description": "Polyvalente et imprévisible. Elle s'adapte à tout et domine dans l'ombre.",
        "color": "#9dc86a",
        "weights": {"aggression": 0.125, "precision": 0.125, "tactics": 0.125, "strategy": 0.125, "defense": 0.125, "chaos": 0.125, "conversion": 0.125, "exploitation": 0.125},
    },
]

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Create the main app without a prefix
app = FastAPI(title="Chess Coach API", version="1.0.0")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")


@app.on_event("startup")
async def startup_event():
    global stockfish_pool, redis_client

    # Pool Stockfish
    stockfish_pool = asyncio.Queue()
    initialized = 0
    for _ in range(STOCKFISH_POOL_SIZE):
        try:
            engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
            engine.configure({"Threads": 1, "Hash": 64})
            await stockfish_pool.put(engine)
            initialized += 1
        except Exception as e:
            logging.warning(f"Erreur init Stockfish engine: {e}")
    logging.info(f"Stockfish pool: {initialized}/{STOCKFISH_POOL_SIZE} engines initialisés")

    # Redis
    try:
        redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logging.info("Redis connecté")
    except Exception as e:
        logging.warning(f"Redis indisponible, cache désactivé: {e}")
        redis_client = None

    # Index MongoDB
    try:
        await db.lichess_puzzles.create_index([("themes", 1), ("rating", 1)])
        await db.user_puzzle_history.create_index([("user_id", 1), ("puzzle_id", 1)])
        await db.user_puzzle_history.create_index([("user_id", 1), ("created_at", -1)])
        await db.player_profiles.create_index([("user_id", 1)])
        await db.user_usage.create_index([("user_id", 1)], unique=True)
    except Exception as e:
        logging.warning(f"Erreur création index: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    if stockfish_pool:
        while not stockfish_pool.empty():
            engine = await stockfish_pool.get()
            try:
                engine.quit()
            except Exception:
                pass
    if redis_client:
        await redis_client.aclose()


# ==================== ENUMS ====================

class GameResult(str, Enum):
    WHITE_WIN = "1-0"
    BLACK_WIN = "0-1"
    DRAW = "1/2-1/2"
    ONGOING = "*"

class MoveClassification(str, Enum):
    BRILLIANT = "brillant"
    EXCELLENT = "excellent"
    GOOD = "bon"
    INACCURACY = "imprécision"
    MISTAKE = "erreur"
    BLUNDER = "gaffe"

class AIStyle(str, Enum):
    AGGRESSIVE = "aggressive"
    POSITIONAL = "positional"
    SOLID = "solid"

# ==================== MODELS ====================

class StatusCheck(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    client_name: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class StatusCheckCreate(BaseModel):
    client_name: str

# Game Models
class MoveAnalysis(BaseModel):
    move_number: int
    move: str
    evaluation: Optional[float] = None
    best_move: Optional[str] = None
    classification: Optional[MoveClassification] = None
    explanation: Optional[str] = None

class Game(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pgn: str
    fen_final: str
    result: GameResult = GameResult.ONGOING
    white_player: str = "Player"
    black_player: str = "AI"
    ai_level: Optional[int] = None
    ai_style: Optional[AIStyle] = None
    moves_count: int = 0
    analysis: List[MoveAnalysis] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    analyzed: bool = False

class DetailedMoveRecord(BaseModel):
    user_id: str
    source: str
    username: Optional[str] = None
    game_id: str
    ply: int
    move_number: int
    phase: str
    eval_before: Optional[float] = None
    eval_after: Optional[float] = None
    delta: Optional[float] = None
    classification: Optional[str] = None
    tags: List[str] = []
    time_spent: Optional[int] = None
    opening_name: Optional[str] = None
    eco: Optional[str] = None
    confidence: float = 0.0
    move_san: Optional[str] = None
    move_uci: Optional[str] = None
    best_move: Optional[str] = None
    fen_before: Optional[str] = None
    fen_after: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class GameCreate(BaseModel):
    pgn: str
    fen_final: str
    result: GameResult = GameResult.ONGOING
    white_player: str = "Player"
    black_player: str = "AI"
    ai_level: Optional[int] = None
    ai_style: Optional[AIStyle] = None
    moves_count: int = 0

# Opening Drill Models
class DrillAttempt(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    opening_id: str
    opening_name: str
    correct_moves: int
    total_moves: int
    errors: int
    completed: bool
    time_spent_seconds: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)

class DrillAttemptCreate(BaseModel):
    opening_id: str
    opening_name: str
    correct_moves: int
    total_moves: int
    errors: int
    completed: bool
    time_spent_seconds: int = 0

# Chess.com Import Models
class ChessComImportRequest(BaseModel):
    user_id: str
    months: int = 3
    max_games: int = 15

class ChessComPlayer(BaseModel):
    username: str
    rating: Optional[int] = None
    result: str

class ChessComImportedGame(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    username: str
    url: str
    pgn: str
    time_class: str
    rated: bool
    white: ChessComPlayer
    black: ChessComPlayer
    end_time: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    analysis: Optional[Dict[str, Any]] = None

# Statistics Models
class OpeningStats(BaseModel):
    opening_id: str
    opening_name: str
    attempts: int = 0
    completions: int = 0
    total_correct: int = 0
    total_errors: int = 0
    mastery_level: float = 0.0  # 0-100%

class PlayerStats(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    total_games: int = 0
    wins: int = 0
    losses: int = 0
    draws: int = 0
    total_moves_played: int = 0
    blunders: int = 0
    mistakes: int = 0
    inaccuracies: int = 0
    brilliant_moves: int = 0
    avg_accuracy: float = 0.0
    opening_stats: Dict[str, OpeningStats] = {}
    favorite_openings: List[str] = []
    weakest_themes: List[str] = []
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class MoveEvalRequest(BaseModel):
    fen_before: str
    fen_after: str
    player_color: str

class CoachMoveRequest(BaseModel):
    fen: str
    elo: int
    style: str


class PuzzleResultRequest(BaseModel):
    user_id: str
    puzzle_id: str
    solved: bool
    attempts: int = 1
    time_ms: Optional[int] = None


# ==================== ROUTES ====================

# Health check
@api_router.get("/")
async def root():
    return {"message": "Chess Coach API", "version": "1.0.0", "status": "healthy"}

@api_router.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# Status routes
@api_router.post("/status", response_model=StatusCheck)
async def create_status_check(input: StatusCheckCreate):
    status_dict = input.model_dump()
    status_obj = StatusCheck(**status_dict)
    _ = await db.status_checks.insert_one(status_obj.model_dump())
    return status_obj

@api_router.get("/status", response_model=List[StatusCheck])
async def get_status_checks():
    status_checks = await db.status_checks.find().to_list(1000)
    return [StatusCheck(**status_check) for status_check in status_checks]

# Game routes
@api_router.post("/games", response_model=Game)
async def create_game(game_data: GameCreate):
    game = Game(**game_data.model_dump())
    await db.games.insert_one(game.model_dump())
    return game

@api_router.get("/games", response_model=List[Game])
async def get_games(limit: int = 50, skip: int = 0):
    games = await db.games.find().sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    return [Game(**game) for game in games]

@api_router.get("/games/{game_id}", response_model=Game)
async def get_game(game_id: str):
    game = await db.games.find_one({"id": game_id})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return Game(**game)

@api_router.put("/games/{game_id}/analysis")
async def update_game_analysis(game_id: str, analysis: List[MoveAnalysis]):
    result = await db.games.update_one(
        {"id": game_id},
        {"$set": {"analysis": [a.model_dump() for a in analysis], "analyzed": True}}
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"message": "Analysis updated"}

# Drill routes
@api_router.post("/drills", response_model=DrillAttempt)
async def create_drill_attempt(drill_data: DrillAttemptCreate):
    drill = DrillAttempt(**drill_data.model_dump())
    await db.drill_attempts.insert_one(drill.model_dump())
    
    # Update opening stats
    await update_opening_stats(drill)
    
    return drill

@api_router.get("/drills", response_model=List[DrillAttempt])
async def get_drill_attempts(opening_id: Optional[str] = None, limit: int = 50):
    query = {}
    if opening_id:
        query["opening_id"] = opening_id
    drills = await db.drill_attempts.find(query).sort("created_at", -1).limit(limit).to_list(limit)
    return [DrillAttempt(**drill) for drill in drills]

@api_router.get("/drills/stats/{opening_id}")
async def get_opening_drill_stats(opening_id: str):
    drills = await db.drill_attempts.find({"opening_id": opening_id}).to_list(1000)
    if not drills:
        return {
            "opening_id": opening_id,
            "attempts": 0,
            "completions": 0,
            "total_correct": 0,
            "total_errors": 0,
            "mastery_level": 0.0,
            "avg_time": 0
        }
    
    completions = sum(1 for d in drills if d.get("completed", False))
    total_correct = sum(d.get("correct_moves", 0) for d in drills)
    total_errors = sum(d.get("errors", 0) for d in drills)
    total_moves = sum(d.get("total_moves", 0) for d in drills)
    total_time = sum(d.get("time_spent_seconds", 0) for d in drills)
    
    mastery = (total_correct / total_moves * 100) if total_moves > 0 else 0
    
    return {
        "opening_id": opening_id,
        "attempts": len(drills),
        "completions": completions,
        "total_correct": total_correct,
        "total_errors": total_errors,
        "mastery_level": round(mastery, 1),
        "avg_time": round(total_time / len(drills), 1) if drills else 0
    }

# Stats routes
@api_router.get("/stats")
async def get_player_stats():
    stats = await db.player_stats.find_one({"id": "global"})
    if not stats:
        return PlayerStats(id="global").model_dump()
    stats.pop("_id", None)
    return PlayerStats(**stats).model_dump()


@api_router.get("/puzzles/pack")
async def get_lichess_puzzle_pack():
    await ensure_lichess_puzzle_cache()

    groups = []
    for label, themes in PUZZLE_GROUPS:
        cursor = db.lichess_puzzles.aggregate([
            {"$match": {"themes": {"$in": themes}, "rating": {"$gte": PUZZLE_RATING_MIN, "$lte": PUZZLE_RATING_MAX}}},
            {"$sample": {"size": 6}},
        ])
        puzzles = []
        async for item in cursor:
            puzzles.append({
                "fen": item.get("fen"),
                "side": item.get("side"),
                "solution": item.get("moves"),
                "themes": item.get("themes", []),
                "rating": item.get("rating"),
            })
        groups.append({"label": label, "puzzles": puzzles})

    return {"groups": groups, "rating_min": PUZZLE_RATING_MIN, "rating_max": PUZZLE_RATING_MAX}


@api_router.get("/puzzles/theme-session")
async def get_theme_puzzle_session(
    themes: str = "",
    elo: int = 1300,
    count: int = 10,
):
    """Retourne une session de puzzles filtrés par thème(s) et difficulté ELO."""
    await ensure_lichess_puzzle_cache()

    theme_list = [t.strip() for t in themes.split(",") if t.strip()] if themes else []
    rating_min = max(400, elo - 150)
    rating_max = min(2500, elo + 250)

    match_filter: dict = {"rating": {"$gte": rating_min, "$lte": rating_max}}
    if theme_list:
        match_filter["themes"] = {"$in": theme_list}

    cursor = db.lichess_puzzles.aggregate([
        {"$match": match_filter},
        {"$sample": {"size": count}},
    ])
    puzzles = []
    async for item in cursor:
        puzzles.append({
            "fen": item.get("fen"),
            "side": item.get("side"),
            "solution": item.get("moves"),
            "themes": item.get("themes", []),
            "rating": item.get("rating"),
            "puzzle_id": item.get("puzzle_id"),
        })

    return {
        "puzzles": puzzles[:count],
        "weaknesses": [],
        "weak_labels": [],
        "elo": elo,
        "personalized": False,
    }


@api_router.get("/puzzles/recommended/{user_id}")
async def get_recommended_puzzles(user_id: str):
    await ensure_lichess_puzzle_cache()

    # 1. Profil joueur
    profile = await db.player_profiles.find_one({"user_id": user_id}, {"_id": 0})
    if not profile:
        return await get_lichess_puzzle_pack()

    weaknesses = profile.get("weaknesses", [])
    elo = profile.get("chesscom_rating") or 1000

    # 2. Thèmes ciblés depuis les faiblesses
    target_themes: List[str] = []
    for w in weaknesses:
        target_themes.extend(WEAKNESS_TO_THEMES.get(w, []))
    target_themes = list(set(target_themes))

    if not target_themes:
        return await get_lichess_puzzle_pack()

    # 3. Exclure les 50 derniers puzzles résolus
    solved_cursor = db.user_puzzle_history.find(
        {"user_id": user_id, "solved": True},
        {"puzzle_id": 1}
    ).sort("created_at", -1).limit(50)
    solved_ids = [doc["puzzle_id"] async for doc in solved_cursor]

    # 4. Rating adaptatif au niveau du joueur
    rating_min = max(400, elo - 100)
    rating_max = min(2500, elo + 200)

    # 5. Puzzles groupés par faiblesse — 10 puzzles total répartis
    active_weaknesses = [w for w in weaknesses if WEAKNESS_TO_THEMES.get(w)]
    puzzles_per_group = max(3, 10 // len(active_weaknesses)) if active_weaknesses else 10
    groups = []
    for weakness in active_weaknesses:
        themes = WEAKNESS_TO_THEMES.get(weakness, [])
        cursor = db.lichess_puzzles.aggregate([
            {"$match": {
                "themes": {"$in": themes},
                "rating": {"$gte": rating_min, "$lte": rating_max},
                "puzzle_id": {"$nin": solved_ids},
            }},
            {"$sample": {"size": puzzles_per_group}},
        ])
        puzzles = []
        async for item in cursor:
            puzzles.append({
                "fen": item.get("fen"),
                "side": item.get("side"),
                "solution": item.get("moves"),
                "themes": item.get("themes", []),
                "rating": item.get("rating"),
                "puzzle_id": item.get("puzzle_id"),
            })
        if puzzles:
            groups.append({
                "label": WEAKNESS_LABELS.get(weakness, weakness.upper()),
                "weakness": weakness,
                "puzzles": puzzles,
            })

    if not groups:
        return await get_lichess_puzzle_pack()

    return {
        "groups": groups,
        "weaknesses": weaknesses,
        "rating_min": rating_min,
        "rating_max": rating_max,
        "personalized": True,
    }


@api_router.get("/puzzles/session/{user_id}")
async def get_puzzle_session(user_id: str):
    """Retourne une session de 10 puzzles plats ciblés sur les faiblesses du joueur."""
    await ensure_lichess_puzzle_cache()

    # 1. Profil joueur
    profile = await db.player_profiles.find_one({"user_id": user_id}, {"_id": 0})
    weaknesses = profile.get("weaknesses", []) if profile else []
    elo = (profile.get("chesscom_rating") or 1500) if profile else 1500

    # 2. Thèmes ciblés
    target_themes: List[str] = []
    for w in weaknesses:
        target_themes.extend(WEAKNESS_TO_THEMES.get(w, []))
    target_themes = list(set(target_themes))

    # Fallback : thèmes tactiques généraux
    if not target_themes:
        target_themes = ["fork", "pin", "mateIn1", "mateIn2", "backRankMate", "deflection"]
        weaknesses = []

    # 3. Exclure puzzles récemment résolus
    solved_cursor = db.user_puzzle_history.find(
        {"user_id": user_id, "solved": True},
        {"puzzle_id": 1}
    ).sort("created_at", -1).limit(50)
    solved_ids = [doc["puzzle_id"] async for doc in solved_cursor]

    # 4. Rating adaptatif
    rating_min = max(400, elo - 150)
    rating_max = min(2500, elo + 250)

    # 5. Tirage de 10 puzzles plats
    cursor = db.lichess_puzzles.aggregate([
        {"$match": {
            "themes": {"$in": target_themes},
            "rating": {"$gte": rating_min, "$lte": rating_max},
            "puzzle_id": {"$nin": solved_ids},
        }},
        {"$sample": {"size": 10}},
    ])
    puzzles = []
    async for item in cursor:
        puzzles.append({
            "fen": item.get("fen"),
            "side": item.get("side"),
            "solution": item.get("moves"),
            "themes": item.get("themes", []),
            "rating": item.get("rating"),
            "puzzle_id": item.get("puzzle_id"),
        })

    # Compléter si moins de 10
    if len(puzzles) < 10:
        extra_cursor = db.lichess_puzzles.aggregate([
            {"$match": {"rating": {"$gte": rating_min, "$lte": rating_max}, "puzzle_id": {"$nin": solved_ids}}},
            {"$sample": {"size": 10 - len(puzzles)}},
        ])
        async for item in extra_cursor:
            puzzles.append({
                "fen": item.get("fen"),
                "side": item.get("side"),
                "solution": item.get("moves"),
                "themes": item.get("themes", []),
                "rating": item.get("rating"),
                "puzzle_id": item.get("puzzle_id"),
            })

    weak_labels = [WEAKNESS_LABELS.get(w, w.upper()) for w in weaknesses]

    return {
        "puzzles": puzzles[:10],
        "weaknesses": weaknesses,
        "weak_labels": weak_labels,
        "elo": elo,
        "personalized": len(weaknesses) > 0,
    }


@api_router.post("/puzzles/result")
async def save_puzzle_result(payload: PuzzleResultRequest):
    await db.user_puzzle_history.insert_one({
        "user_id": payload.user_id,
        "puzzle_id": payload.puzzle_id,
        "solved": payload.solved,
        "attempts": payload.attempts,
        "time_ms": payload.time_ms,
        "created_at": datetime.utcnow(),
    })
    return {"status": "ok"}


@api_router.post("/stats/update")
async def update_player_stats(updates: Dict[str, Any]):
    await db.player_stats.update_one(
        {"id": "global"},
        {"$set": {**updates, "updated_at": datetime.utcnow()}},
        upsert=True
    )
    return {"message": "Stats updated"}

# Chess.com Import Routes
async def fetch_chesscom_json(url: str) -> Dict[str, Any]:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        response = await run_in_threadpool(requests.get, url, headers=headers, timeout=12)
    except Exception:
        raise HTTPException(status_code=502, detail="Chess.com indisponible")

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Utilisateur Chess.com introuvable")
    if not response.ok:
        raise HTTPException(status_code=502, detail="Erreur Chess.com")
    return response.json()

def analyze_pgn_simple(
    pgn: str,
    username: str,
    time_limit: float = STOCKFISH_TIME_LIMIT,
    depth: int = STOCKFISH_DEPTH,
) -> Dict[str, Any]:
    try:
        game = chess.pgn.read_game(io.StringIO(pgn))
    except Exception:
        return {"blunders": 0, "mistakes": 0, "inaccuracies": 0, "player_moves": 0, "accuracy": 0.0}

    if not game:
        return {"blunders": 0, "mistakes": 0, "inaccuracies": 0, "player_moves": 0, "accuracy": 0.0}

    white_player = (game.headers.get("White") or "").lower()
    black_player = (game.headers.get("Black") or "").lower()
    username_lower = username.lower()

    if white_player == username_lower:
        player_color = chess.WHITE
    elif black_player == username_lower:
        player_color = chess.BLACK
    else:
        return {"blunders": 0, "mistakes": 0, "inaccuracies": 0, "player_moves": 0, "accuracy": 0.0}

    board = game.board()
    blunders = 0
    mistakes = 0
    inaccuracies = 0
    player_moves = 0

    for move in game.mainline_moves():
        mover_color = board.turn
        is_player_move = mover_color == player_color
        to_square = move.to_square

        board.push(move)

        if is_player_move:
            player_moves += 1
            opponent_color = chess.BLACK if player_color == chess.WHITE else chess.WHITE
            if board.is_attacked_by(opponent_color, to_square):
                if not board.is_attacked_by(player_color, to_square):
                    blunders += 1
                else:
                    mistakes += 1

    penalty = blunders * 5 + mistakes * 2 + inaccuracies
    accuracy = 0.0
    if player_moves > 0:
        accuracy = max(0.0, round(100 - (penalty / player_moves * 10), 1))

    return {
        "blunders": blunders,
        "mistakes": mistakes,
        "inaccuracies": inaccuracies,
        "player_moves": player_moves,
        "accuracy": accuracy,
    }

stockfish_pool: asyncio.Queue = None
redis_client: aioredis.Redis = None


async def analyze_position_stockfish(fen: str) -> Dict[str, Any]:
    # 1. Cache Redis
    cache_key = f"sf:{hashlib.md5(fen.encode()).hexdigest()}"
    if redis_client:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception:
            pass

    # 2. Engine du pool
    engine = await stockfish_pool.get()
    try:
        def _analyze():
            board = chess.Board(fen)
            limit = chess.engine.Limit(time=STOCKFISH_TIME_LIMIT, depth=STOCKFISH_DEPTH)
            info = engine.analyse(board, limit)
            score = info["score"].white()
            mate = score.mate()
            # mate >= 0 covers Mate(0) = checkmate already delivered (White wins)
            cp = score.cp if mate is None else (10000 if mate >= 0 else -10000)
            pv = info.get("pv", [])
            best_move = pv[0].uci() if pv else None
            return {"cp": cp, "mate": mate, "best_move": best_move}

        result = await run_in_threadpool(_analyze)
    finally:
        await stockfish_pool.put(engine)

    # 3. Mettre en cache
    if redis_client:
        try:
            await redis_client.setex(cache_key, REDIS_TTL_EVAL, json.dumps(result))
        except Exception:
            pass

    return result

def classify_move_cpl(cpl: float) -> str:
    """Used for batch game analysis (post-import). Strict thresholds."""
    if cpl <= 15:
        return "best"
    if cpl <= 50:
        return "ok"
    if cpl <= 100:
        return "inaccuracy"
    if cpl <= 300:
        return "mistake"
    return "blunder"


def classify_move_cpl_live(cpl: float) -> str:
    """Used for real-time coach evaluation. More lenient thresholds to avoid
    false positives due to shallow analysis (80ms depth-12 on Railway)."""
    if cpl <= 20:
        return "best"
    if cpl <= 60:
        return "ok"
    if cpl <= 130:
        return "inaccuracy"
    if cpl <= 400:
        return "mistake"
    return "blunder"

def get_time_weight(time_class: str) -> float:
    mapping = {
        "bullet": 0.6,
        "blitz": 0.8,
        "rapid": 1.0,
        "daily": 1.0,
    }
    return mapping.get(time_class, 1.0)

def get_result_weight(result: str, player_is_white: bool) -> float:
    if result == "1-0":
        return 0.9 if player_is_white else 1.1
    if result == "0-1":
        return 1.1 if player_is_white else 0.9
    return 1.0

def get_recency_weight(end_time: int) -> float:
    if not end_time:
        return 1.0
    days = max(0, (datetime.utcnow().timestamp() - end_time) / 86400)
    factor = min(days, 90) / 90
    return max(0.6, 1 - factor * 0.4)

def get_opponent_weight(opponent_rating: Optional[int]) -> float:
    if not opponent_rating:
        return 1.0
    return max(0.6, min(1.2, opponent_rating / 1500))

def get_elo_error_rate(elo: int) -> float:
    elo = max(400, min(1500, elo))
    if elo >= 1400:
        return 0.01
    base = (1500 - elo) / 1100
    return max(0.02, min(0.55, 0.02 + base * 0.45))

def score_axis(penalty: float, moves: int, factor: float = 0.8) -> float:
    rate = penalty / max(1, moves)
    return round(100 * math.exp(-rate / factor), 1)

def axis_to_elo(score: float) -> int:
    return int(100 + (score / 100) * 1400)

def parse_clock_seconds(comment: str) -> Optional[int]:
    if not comment:
        return None
    match = re.search(r"%clk\s+(\d+):(\d+):(\d+)", comment)
    if not match:
        return None
    hours, minutes, seconds = map(int, match.groups())
    return hours * 3600 + minutes * 60 + seconds

def get_game_outcome(pgn: str, username: str) -> str:
    game = chess.pgn.read_game(io.StringIO(pgn))
    if not game:
        return "unknown"
    result = game.headers.get("Result")
    white_player = (game.headers.get("White") or "").lower()
    black_player = (game.headers.get("Black") or "").lower()
    is_white = white_player == username.lower()
    if result == "1-0":
        return "win" if is_white else "loss"
    if result == "0-1":
        return "loss" if is_white else "win"
    if result == "1/2-1/2":
        return "draw"
    return "unknown"

def get_openai_client() -> Optional[OpenAI]:
    api_key = os.getenv("EMERGENT_LLM_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    return OpenAI(api_key=api_key)

async def generate_ai_report(payload: Dict[str, Any]) -> Dict[str, Any]:
    client = get_openai_client()
    if not client:
        return {}

    def _call():
        system_prompt = (
            "Tu es Coach Rasta, un coach d’échecs francophone au style street et décalé. "
            "Tu parles avec humour, un peu trash, façon grande gueule bienveillante — tu taquines mais tu aides vraiment. "
            "Tu utilises des expressions familières françaises (fréro, t’as vu, c’est chaud, etc.) mais reste compréhensible. "
            "Jamais vulgaire, mais pas politiquement correct non plus. Tes conseils sont concrets et directs. "
            "Retourne uniquement un JSON valide en français."
        )
        user_prompt = (
            "Génère une analyse à partir des stats suivantes. "
            "Contraintes: 1) detailed_report: 2-4 paragraphes dans le style Coach Rasta, avec au moins 1 métrique par paragraphe. "
            "2) short_summary: 1-2 phrases max, style Coach Rasta punchline. "
            "3) strengths: 2 éléments max avec title et detail. "
            "4) weaknesses: 3 éléments max avec title, detail et advice. "
            "5) Tout en français.\n\n"
            f"STATS_JSON={json.dumps(payload, ensure_ascii=False)}"
        )

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.4,
        )
        content = response.choices[0].message.content or ""
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, re.S)
            if match:
                return json.loads(match.group(0))
            return {}

    return await run_in_threadpool(_call)

def compute_tag_stats(move_records: List[Dict[str, Any]], recent_game_ids: set, previous_game_ids: set) -> Dict[str, Any]:
    tag_stats: Dict[str, Dict[str, Any]] = {}
    total_moves = len(move_records)
    recent_moves = sum(1 for m in move_records if m.get("game_id") in recent_game_ids)
    previous_moves = sum(1 for m in move_records if m.get("game_id") in previous_game_ids)

    for record in move_records:
        delta = record.get("delta") or 0
        for tag in record.get("tags", []):
            stat = tag_stats.setdefault(tag, {
                "count": 0,
                "severity": 0.0,
                "recent_count": 0,
                "previous_count": 0,
            })
            stat["count"] += 1
            stat["severity"] += abs(delta)
            if record.get("game_id") in recent_game_ids:
                stat["recent_count"] += 1
            elif record.get("game_id") in previous_game_ids:
                stat["previous_count"] += 1

    for tag, stat in tag_stats.items():
        stat["rate"] = round(stat["count"] / max(1, total_moves), 3)
        recent_rate = stat["recent_count"] / max(1, recent_moves)
        prev_rate = stat["previous_count"] / max(1, previous_moves)
        stat["recent_trend"] = round(recent_rate - prev_rate, 3)

    return tag_stats

def compute_phase_summary(move_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary = {}
    worst_phase = None
    worst_accuracy = 999
    for phase in ["opening", "middlegame", "endgame"]:
        phase_moves = [m for m in move_records if m.get("phase") == phase]
        if not phase_moves:
            summary[phase] = {"accuracy": 0, "blunder_rate": 0, "mistake_rate": 0}
            continue
        cpl_total = sum(max(0, -(m.get("delta") or 0)) for m in phase_moves)
        avg_cpl = cpl_total / max(1, len(phase_moves))
        accuracy = max(0.0, round(100 - (avg_cpl / 10), 1))
        blunders = sum(1 for m in phase_moves if m.get("classification") == "blunder")
        mistakes = sum(1 for m in phase_moves if m.get("classification") == "mistake")
        blunder_rate = round(blunders / max(1, len(phase_moves)), 3)
        mistake_rate = round(mistakes / max(1, len(phase_moves)), 3)
        summary[phase] = {
            "accuracy": accuracy,
            "blunder_rate": blunder_rate,
            "mistake_rate": mistake_rate,
        }
        if accuracy < worst_accuracy:
            worst_accuracy = accuracy
            worst_phase = phase

    summary["worst_phase"] = worst_phase
    return summary

def compute_opening_stats(game_stats: List[Dict[str, Any]], move_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    opening_stats: Dict[str, Dict[str, Any]] = {}
    game_opening = {g["game_id"]: g.get("opening_name") or "Inconnue" for g in game_stats}

    for game in game_stats:
        opening = game.get("opening_name") or "Inconnue"
        stat = opening_stats.setdefault(opening, {
            "games": 0,
            "wins": 0,
            "winrate": 0,
            "opening_accuracy": 0,
            "early_blunder_rate": 0,
            "deviation_ply_common": None,
        })
        stat["games"] += 1
        if game.get("outcome") == "win":
            stat["wins"] += 1

    # opening accuracy + early blunders
    opening_moves: Dict[str, List[Dict[str, Any]]] = {}
    for move in move_records:
        opening = game_opening.get(move.get("game_id"), "Inconnue")
        if move.get("move_number", 0) <= 12:
            opening_moves.setdefault(opening, []).append(move)

    for opening, moves in opening_moves.items():
        if opening not in opening_stats:
            continue
        cpl_total = sum(max(0, -(m.get("delta") or 0)) for m in moves)
        avg_cpl = cpl_total / max(1, len(moves))
        opening_stats[opening]["opening_accuracy"] = round(max(0.0, 100 - (avg_cpl / 10)), 1)

    for opening, stat in opening_stats.items():
        game_ids = [g["game_id"] for g in game_stats if (g.get("opening_name") or "Inconnue") == opening]
        blunder_games = 0
        deviation_counts: Dict[int, int] = {}
        for game_id in game_ids:
            game_moves = [m for m in move_records if m.get("game_id") == game_id and m.get("move_number", 0) <= 12]
            deviation_ply = None
            for m in game_moves:
                if m.get("classification") in ["mistake", "blunder"]:
                    deviation_ply = m.get("move_number")
                    break
            if deviation_ply:
                deviation_counts[deviation_ply] = deviation_counts.get(deviation_ply, 0) + 1
            if any(m.get("classification") == "blunder" for m in game_moves):
                blunder_games += 1
        stat["early_blunder_rate"] = round(blunder_games / max(1, len(game_ids)), 3)
        if deviation_counts:
            stat["deviation_ply_common"] = max(deviation_counts.items(), key=lambda x: x[1])[0]

    for opening, stat in opening_stats.items():
        stat["winrate"] = round(stat["wins"] / max(1, stat["games"]), 3)

    return opening_stats

def build_detailed_report(summary: Dict[str, Any], phase_summary: Dict[str, Any], tag_stats: Dict[str, Any], opening_stats: Dict[str, Any]) -> str:
    games_played = summary.get("games_played", 0)
    winrate = round(summary.get("winrate", 0) * 100, 1)
    avg_accuracy = summary.get("avg_accuracy", 0)
    avg_blunders = summary.get("avg_blunders_per_game", 0)
    avg_mistakes = summary.get("avg_mistakes_per_game", 0)
    trend = summary.get("accuracy_trend", 0)

    tag_labels = {
        "HANGING_PIECE": "pièces en prise",
        "IMPULSIVE": "jeu trop rapide",
        "MISSED_THREAT": "menaces adverses ratées",
        "KING_SAFETY": "roi exposé",
        "OPENING_PRINCIPLE": "principes d’ouverture",
        "TACTICAL_MISS": "tactique ratée",
        "CONVERSION_ERROR": "conversion d’avantage",
        "TILT_CHAIN": "tilt après erreur",
    }

    top_tags = sorted(tag_stats.items(), key=lambda x: x[1].get("severity", 0), reverse=True)
    main_tag = top_tags[0][0] if top_tags else None
    main_tag_label = tag_labels.get(main_tag, "gaffes") if main_tag else "gaffes"

    openings_sorted = sorted(opening_stats.items(), key=lambda x: x[1].get("games", 0), reverse=True)
    opening_line = ""
    if openings_sorted:
        top_three = ", ".join([f"{o[0]} ({o[1].get('games', 0)} parties)" for o in openings_sorted[:3]])
        opening_line = f"Ouvertures les plus jouées: {top_three}."

    worst_opening = None
    if openings_sorted:
        worst_opening = sorted(openings_sorted, key=lambda x: (x[1].get("winrate", 0), -x[1].get("early_blunder_rate", 0)))[0][0]

    worst_phase = phase_summary.get("worst_phase")
    worst_phase_text = f"Ta phase la plus fragile est le {worst_phase}." if worst_phase else ""

    report = (
        f"Sur tes {games_played} dernières parties, tu as {winrate}% de victoires et une précision moyenne de {avg_accuracy}%. "
        f"Tu fais en moyenne {avg_blunders} gaffes et {avg_mistakes} erreurs par partie. "
    )
    if trend:
        report += f"Ta précision a évolué de {trend:+.1f} points sur les 10 dernières parties. "
    report += f"Ton principal frein, c’est {main_tag_label}. "
    if worst_phase_text:
        report += worst_phase_text + " "
    if opening_line:
        report += opening_line + " "
    if worst_opening:
        report += f"Ouverture à travailler: {worst_opening}. "

    report += "Plan d’action: (1) check-list menaces avant chaque coup, (2) ralentir ≥5s sur les coups non forcés, (3) drills ouverture 10 min/jour." 
    return report

def build_local_ai_report(summary: Dict[str, Any], phase_summary: Dict[str, Any], tag_stats: Dict[str, Any], opening_stats: Dict[str, Any]) -> Dict[str, Any]:
    games_played = summary.get("games_played", 0)
    winrate = round(summary.get("winrate", 0) * 100, 1)
    avg_accuracy = summary.get("avg_accuracy", 0)
    avg_blunders = summary.get("avg_blunders_per_game", 0)
    avg_mistakes = summary.get("avg_mistakes_per_game", 0)
    trend = summary.get("accuracy_trend", 0)

    tag_labels = {
        "HANGING_PIECE": "Pièces en prise",
        "IMPULSIVE": "Jeu trop rapide",
        "MISSED_THREAT": "Menaces adverses ratées",
        "KING_SAFETY": "Roi exposé",
        "OPENING_PRINCIPLE": "Principes d’ouverture",
        "TACTICAL_MISS": "Tactique ratée",
        "CONVERSION_ERROR": "Conversion d’avantage",
        "TILT_CHAIN": "Tilt après erreur",
    }
    tag_advice = {
        "HANGING_PIECE": "Avant de jouer, scanne toutes tes pièces et vérifie si elles sont défendues.",
        "IMPULSIVE": "Ralentis: minimum 5 secondes sur les coups non forcés.",
        "MISSED_THREAT": "Regarde les menaces adverses avant de cliquer: captures et échecs d’abord.",
        "KING_SAFETY": "Roque tôt et évite d’ouvrir les pions du roi sans raison.",
        "OPENING_PRINCIPLE": "Développe tes pièces et évite de rejouer la même trop tôt.",
        "TACTICAL_MISS": "Cherche les tactiques simples: fourchettes, clouages, découvertes.",
        "CONVERSION_ERROR": "Quand tu es mieux, échange les pièces et simplifie.",
        "TILT_CHAIN": "Après une erreur, joue un coup simple et solide.",
    }

    top_tags = sorted(tag_stats.items(), key=lambda x: x[1].get("severity", 0), reverse=True)
    weaknesses = []
    for tag, stat in top_tags[:3]:
        weaknesses.append({
            "title": tag_labels.get(tag, tag),
            "detail": f"Taux {stat.get('rate', 0) * 100:.1f}% · Gravité {stat.get('severity', 0):.0f}",
            "advice": tag_advice.get(tag, "Travaille ce point avec des exercices simples."),
        })

    strengths = []
    phase_items = [(k, v) for k, v in phase_summary.items() if k in ["opening", "middlegame", "endgame"]]
    phase_items.sort(key=lambda x: x[1].get("accuracy", 0), reverse=True)
    for phase, data in phase_items[:2]:
        strengths.append({
            "title": f"Phase {phase}",
            "detail": f"Précision {data.get('accuracy', 0)}% · Blunders {data.get('blunder_rate', 0) * 100:.1f}%",
        })

    openings_sorted = sorted(opening_stats.items(), key=lambda x: x[1].get("games", 0), reverse=True)
    top_openings = ", ".join([f"{o[0]} ({o[1].get('games', 0)} parties)" for o in openings_sorted[:3]])
    worst_opening = None
    if openings_sorted:
        worst_opening = sorted(openings_sorted, key=lambda x: (x[1].get("winrate", 0), -x[1].get("early_blunder_rate", 0)))[0]

    fast_blunder_rate = summary.get("fast_blunder_rate")
    threat_miss_rate = summary.get("threat_miss_rate")
    advantage_loss_rate = summary.get("advantage_loss_rate")

    main_weak = weaknesses[0]["title"] if weaknesses else "les erreurs grossières"
    phase_labels = {"opening": "ouverture", "middlegame": "milieu de jeu", "endgame": "finale"}
    worst_phase_label = phase_labels.get(phase_summary.get("worst_phase")) if phase_summary else None

    detailed_report = (
        f"Sur tes {games_played} dernières parties, tu as {winrate}% de victoires et une précision moyenne de {avg_accuracy}%. "
        f"Tu fais en moyenne {avg_blunders} gaffes et {avg_mistakes} erreurs par partie. "
        f"La précision a évolué de {trend:+.1f} points sur les 10 dernières parties.\n\n"
        f"Ton principal frein, c’est {main_weak}. "
        + (f"{fast_blunder_rate * 100:.0f}% de tes gaffes arrivent trop vite. " if fast_blunder_rate is not None else "")
        + (f"Tu rates les menaces directes dans {threat_miss_rate * 100:.0f}% des erreurs. " if threat_miss_rate is not None else "")
        + (f"Tu perds des positions gagnantes dans {advantage_loss_rate * 100:.0f}% des cas. " if advantage_loss_rate is not None else "")
        + "\n\n"
        + (f"Points forts: {strengths[0]['detail']}" if strengths else "")
        + (f" · {strengths[1]['detail']}" if len(strengths) > 1 else "")
        + ". "
        + (f"Ta phase la plus fragile est l’{worst_phase_label}." if worst_phase_label else "")
        + "\n\n"
        + (f"Ouvertures les plus jouées: {top_openings}. " if top_openings else "")
        + (f"Ouverture à travailler: {worst_opening[0]} (winrate {worst_opening[1].get('winrate', 0) * 100:.0f}%, gaffes tôt {worst_opening[1].get('early_blunder_rate', 0) * 100:.0f}%). " if worst_opening else "")
        + "\n\n"
        + "Plan d’action clair: "
        + "(1) liste des menaces avant chaque coup, objectif 5 parties sans pièce offerte; "
        + "(2) ralentir à 5 secondes sur les coups non forcés; "
        + "(3) exercices d’ouverture 10 minutes par jour pendant 7 jours."
    )

    short_summary = (
        f"Yo fréro, t'as {avg_accuracy}% de précision et {avg_blunders} gaffes par partie — "
        f"c'est ton {main_weak} qui te plombe. T'as {winrate}% de wins, mais faut bosser frère !"
    )

    return {
        "detailed_report": detailed_report,
        "short_summary": short_summary,
        "strengths": strengths,
        "weaknesses": weaknesses,
    }

def _tag_to_puzzle_prompt(tag: str) -> str:
    prompts = {
        "HANGING_PIECE": "Trouve la pièce en prise et gagne du matériel.",
        "MISSED_THREAT": "Quel est le meilleur coup défensif pour éviter la menace ?",
        "KING_SAFETY": "Trouve le coup qui attaque le roi exposé.",
        "OPENING_PRINCIPLE": "Trouve le coup simple qui respecte les principes d’ouverture.",
        "TACTICAL_MISS": "Trouve la tactique gagnante en 2 à 4 coups.",
        "CONVERSION_ERROR": "Simplifie en gagnant: trouve l’échange le plus sûr.",
        "TILT_CHAIN": "Stabilise la position avec un coup solide.",
    }
    return prompts.get(tag, "Trouve le meilleur coup.")

async def generate_puzzle_pack(move_records: List[Dict[str, Any]], avg_accuracy: float) -> Dict[str, Any]:
    if not move_records:
        return {"summary": "", "puzzles": []}

    max_ply = 2
    if avg_accuracy >= 75:
        max_ply = 4
    elif avg_accuracy >= 60:
        max_ply = 3

    candidates = [
        m for m in move_records
        if m.get("delta") is not None
        and m.get("delta") <= -150
        and m.get("classification") in ["mistake", "blunder"]
        and (m.get("eval_before") or 0) > -300
        and m.get("fen_before")
    ]

    if len(candidates) < 8:
        fallback = [
            m for m in move_records
            if m.get("delta") is not None
            and m.get("delta") <= -120
            and m.get("classification") in ["inaccuracy", "mistake", "blunder"]
            and (m.get("eval_before") or 0) > -400
            and m.get("fen_before")
        ]
        seen = {m.get("fen_before") for m in candidates}
        for m in fallback:
            if m.get("fen_before") not in seen:
                candidates.append(m)
                seen.add(m.get("fen_before"))

    if len(candidates) < 6:
        extra = [
            m for m in move_records
            if m.get("delta") is not None
            and m.get("delta") <= -60
            and m.get("classification") in ["inaccuracy", "mistake", "blunder"]
            and (m.get("eval_before") or 0) > -500
            and m.get("fen_before")
        ]
        seen = {m.get("fen_before") for m in candidates}
        for m in extra:
            if m.get("fen_before") not in seen:
                candidates.append(m)
                seen.add(m.get("fen_before"))

    if not candidates:
        return {"summary": "", "puzzles": []}

    # tag dominance
    tag_severity: Dict[str, float] = {}
    for m in candidates:
        for tag in m.get("tags", []):
            tag_severity[tag] = tag_severity.get(tag, 0) + abs(m.get("delta") or 0)

    sorted_tags = sorted(tag_severity.items(), key=lambda x: x[1], reverse=True)
    main_tag = sorted_tags[0][0] if sorted_tags else None
    secondary_tag = sorted_tags[1][0] if len(sorted_tags) > 1 else None

    pack_size = max(8, min(12, len(candidates)))
    target_main = max(1, round(pack_size * 0.7))
    target_secondary = max(1, round(pack_size * 0.2))
    target_strength = max(1, pack_size - target_main - target_secondary)

    by_tag: Dict[str, List[Dict[str, Any]]] = {}
    for m in candidates:
        tag = m.get("tags")[0] if m.get("tags") else "TACTICAL_MISS"
        by_tag.setdefault(tag, []).append(m)

    selected: List[Dict[str, Any]] = []

    def pick_from(tag: Optional[str], count: int):
        if not tag or tag not in by_tag:
            return
        pool = by_tag[tag]
        while pool and len([s for s in selected if s.get("tag") == tag]) < count:
            selected.append(pool.pop(0))

    pick_from(main_tag, target_main)
    pick_from(secondary_tag, target_secondary)

    # fill remaining with any
    remaining = pack_size - len(selected)
    if remaining > 0:
        for tag, pool in by_tag.items():
            while pool and remaining > 0:
                selected.append(pool.pop(0))
                remaining -= 1

    puzzles: List[Dict[str, Any]] = []
    engine = None
    try:
        engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
        for m in selected:
            fen = m.get("fen_before")
            if not fen:
                continue
            board = chess.Board(fen)
            analysis = engine.analyse(board, chess.engine.Limit(time=0.15), info=chess.engine.INFO_PV)
            pv = analysis.get("pv", [])
            if pv and len(pv) > max_ply and len(selected) >= 8:
                continue
            best = m.get("best_move")
            if not best and pv:
                best = pv[0].uci()
            tags = m.get("tags") or ["TACTICAL_MISS"]
            puzzles.append({
                "fen": fen,
                "best_move": best,
                "tag": tags[0],
                "phase": m.get("phase"),
                "prompt": _tag_to_puzzle_prompt(tags[0]),
            })
    finally:
        if engine:
            engine.quit()

    if not puzzles:
        for m in selected:
            fen = m.get("fen_before")
            if not fen:
                continue
            tags = m.get("tags") or ["TACTICAL_MISS"]
            puzzles.append({
                "fen": fen,
                "best_move": m.get("best_move"),
                "tag": tags[0],
                "phase": m.get("phase"),
                "prompt": _tag_to_puzzle_prompt(tags[0]),
            })

    label_map = {
        "HANGING_PIECE": "les pièces en prise",
        "IMPULSIVE": "le jeu trop rapide",
        "MISSED_THREAT": "les menaces ratées",
        "KING_SAFETY": "la sécurité du roi",
        "OPENING_PRINCIPLE": "les principes d’ouverture",
        "TACTICAL_MISS": "la tactique",
        "CONVERSION_ERROR": "la conversion d’avantage",
        "TILT_CHAIN": "le tilt",
    }
    summary = ""
    if main_tag:
        summary = f"Sur tes 10 dernières parties, la faiblesse principale est {label_map.get(main_tag, main_tag)}. On va te donner {len(puzzles)} positions ciblées."

    return {"summary": summary, "puzzles": puzzles}


def _load_lichess_puzzles_sync() -> List[Dict[str, Any]]:
    response = requests.get(
        PUZZLE_DB_URL,
        stream=True,
        timeout=(10, 120),
        headers={"User-Agent": "ChessCoach/1.0"},
    )
    response.raise_for_status()
    dctx = zstd.ZstdDecompressor()
    stream_reader = dctx.stream_reader(response.raw)
    text_stream = io.TextIOWrapper(stream_reader, encoding="utf-8")
    reader = csv.reader(text_stream)

    puzzles: List[Dict[str, Any]] = []
    seen_ids: set = set()

    for row in reader:
        if len(row) < 8:
            continue
        puzzle_id = row[0]
        if puzzle_id in seen_ids:
            continue
        fen = row[1]
        moves = row[2]
        try:
            rating = int(row[3])
        except Exception:
            continue
        if rating < PUZZLE_RATING_MIN or rating > PUZZLE_RATING_MAX:
            continue
        themes = row[7].split()
        game_url = row[8] if len(row) > 8 else None
        side = fen.split(" ")[1] if fen else "w"

        puzzles.append({
            "puzzle_id": puzzle_id,
            "fen": fen,
            "moves": moves,
            "rating": rating,
            "themes": themes,
            "side": side,
            "game_url": game_url,
        })
        seen_ids.add(puzzle_id)

        if len(puzzles) >= PUZZLE_CACHE_SIZE:
            break

    return puzzles


async def ensure_lichess_puzzle_cache():
    count = await db.lichess_puzzles.count_documents({})
    if count >= min(PUZZLE_CACHE_SIZE, 1000):
        return

    try:
        puzzles = await asyncio.to_thread(_load_lichess_puzzles_sync)
    except Exception as exc:
        print(f"Erreur chargement puzzles Lichess: {exc}")
        return

    if not puzzles:
        return

    await db.lichess_puzzles.delete_many({})
    await db.lichess_puzzles.insert_many(puzzles)

def get_phase_from_ply(ply_index: int) -> str:
    if ply_index < 20:
        return "opening"
    if ply_index < 60:
        return "middlegame"
    return "endgame"

async def analyze_game_stockfish(
    pgn: str,
    username: str,
    time_class: str,
    end_time: int,
    opponent_rating: Optional[int],
    result: str,
    time_limit: float = STOCKFISH_TIME_LIMIT,
    depth: int = STOCKFISH_DEPTH,
    max_plies: Optional[int] = None,
) -> Dict[str, Any]:
    def _analyze_game():
        game = chess.pgn.read_game(io.StringIO(pgn))
        if not game:
            return {"analysis": None, "penalties": None, "phase": None, "moves": 0}

        white_player = (game.headers.get("White") or "").lower()
        black_player = (game.headers.get("Black") or "").lower()
        username_lower = username.lower()

        if white_player == username_lower:
            player_color = chess.WHITE
            player_is_white = True
        elif black_player == username_lower:
            player_color = chess.BLACK
            player_is_white = False
        else:
            return {"analysis": None, "penalties": None, "phase": None, "moves": 0}

        weight = (
            get_recency_weight(end_time)
            * get_time_weight(time_class)
            * get_opponent_weight(opponent_rating)
            * get_result_weight(result, player_is_white)
        )

        engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
        try:
            try:
                engine.configure({"Threads": 1, "Hash": 64})
            except Exception:
                pass

            board = game.board()
            ply_index = 0
            player_moves = 0
            blunders = 0
            mistakes = 0
            inaccuracies = 0
            total_cpl = 0.0

            penalties = {
                "discipline": 0.0,
                "king_safety": 0.0,
                "tactics": 0.0,
                "structure": 0.0,
                "conversion": 0.0,
            }

            phase_stats = {
                "opening": {"cpl": 0.0, "moves": 0},
                "middlegame": {"cpl": 0.0, "moves": 0},
                "endgame": {"cpl": 0.0, "moves": 0},
            }

            moved_piece_counts: Dict[str, int] = {}
            minor_developed = set()
            castled = False
            prev_clock = None
            last_blunder_ply = None
            advantage_positions = 0
            metrics = {
                "fast_moves": 0,
                "fast_blunders": 0,
                "threat_miss": 0,
                "hanging_blunders": 0,
                "late_castle": 0,
                "king_exposure": 0,
                "advantage_loss": 0,
                "tilt_chain": 0,
                "advantage_positions": 0,
            }
            move_records: List[Dict[str, Any]] = []
            opening_name = game.headers.get("Opening")
            eco = game.headers.get("ECO")
            opening_confidence = 0.9 if opening_name else 0.2

            def evaluate_board() -> Dict[str, Any]:
                limit = chess.engine.Limit(time=time_limit, depth=depth)
                info = engine.analyse(board, limit)
                score = info["score"].white()
                mate = score.mate()
                cp = score.cp if mate is None else None
                if mate is not None:
                    # mate >= 0: covers Mate(0) = checkmate already delivered (White wins)
                    cp = 10000 if mate >= 0 else -10000
                pv = info.get("pv", [])
                best_move = pv[0].uci() if pv else None
                return {"cp": cp, "mate": mate, "best_move": best_move}

            for node_index, node in enumerate(game.mainline()):
                if max_plies is not None and node_index >= max_plies:
                    break
                move = node.move
                is_player_move = board.turn == player_color

                fen_before = board.fen()
                piece = board.piece_at(move.from_square)
                san = board.san(move)

                if is_player_move:
                    eval_before = evaluate_board()
                    best_move = eval_before.get("best_move")
                    mate_before = eval_before.get("mate")
                    cp_before = eval_before.get("cp")
                    if cp_before is not None and ((player_is_white and cp_before > 200) or (not player_is_white and cp_before < -200)):
                        advantage_positions += 1

                board.push(move)

                if is_player_move:
                    player_moves += 1
                    eval_after = evaluate_board()
                    cp_after = eval_after.get("cp")
                    best_reply = eval_after.get("best_move")
                    move_cpl = 0.0
                    eval_before_player = None
                    eval_after_player = None
                    delta = None

                    if cp_before is not None and cp_after is not None:
                        if player_is_white:
                            eval_before_player = cp_before
                            eval_after_player = cp_after
                            move_cpl = max(0.0, cp_before - cp_after)
                        else:
                            eval_before_player = -cp_before
                            eval_after_player = -cp_after
                            move_cpl = max(0.0, cp_after - cp_before)
                        delta = eval_after_player - eval_before_player

                    total_cpl += move_cpl
                    classification = classify_move_cpl(move_cpl)

                    if classification == "inaccuracy":
                        inaccuracies += 1
                        penalties["discipline"] += weight * 1
                    elif classification == "mistake":
                        mistakes += 1
                        penalties["discipline"] += weight * 2
                    elif classification == "blunder":
                        blunders += 1
                        penalties["discipline"] += weight * 3

                    phase = get_phase_from_ply(ply_index)
                    phase_stats[phase]["cpl"] += move_cpl * weight
                    phase_stats[phase]["moves"] += 1

                    time_spent = None
                    if node.comment:
                        current_clock = parse_clock_seconds(node.comment)
                        if current_clock is not None and prev_clock is not None:
                            time_spent = prev_clock - current_clock
                            if time_spent < 5:
                                metrics["fast_moves"] += 1
                                if classification in ["mistake", "blunder"] and move_cpl >= 150:
                                    metrics["fast_blunders"] += 1
                        if current_clock is not None:
                            prev_clock = current_clock

                    if piece and piece.piece_type == chess.KING:
                        penalties["king_safety"] += weight * 1.5
                    if board.is_check():
                        penalties["king_safety"] += weight * 1.0
                    if not castled and san in ["O-O", "O-O-O"]:
                        castled = True
                        if ply_index >= 20:
                            penalties["king_safety"] += weight * 1.2

                    if piece and piece.piece_type == chess.PAWN:
                        file_idx = chess.square_file(move.from_square)
                        if file_idx in [5, 6, 7]:
                            penalties["king_safety"] += weight * 0.5

                    if board.is_attacked_by(not player_color, move.to_square) and not board.is_attacked_by(player_color, move.to_square):
                        penalties["discipline"] += weight * 1.5
                        if classification in ["mistake", "blunder"]:
                            metrics["hanging_blunders"] += 1

                    if mate_before is not None and mate_before > 0 and (best_move != move.uci()):
                        penalties["tactics"] += weight * 2.5
                    if move_cpl >= 200:
                        penalties["tactics"] += weight * 1.5
                    if best_reply:
                        reply_move = chess.Move.from_uci(best_reply)
                        if (board.is_capture(reply_move) or board.gives_check(reply_move)) and classification in ["mistake", "blunder"]:
                            metrics["threat_miss"] += 1

                    if piece and ply_index < 20 and piece.piece_type in [chess.KNIGHT, chess.BISHOP]:
                        minor_developed.add(move.from_square)
                    piece_key = f"{move.from_square}-{piece.piece_type if piece else 'x'}"
                    moved_piece_counts[piece_key] = moved_piece_counts.get(piece_key, 0) + 1
                    if ply_index < 20 and moved_piece_counts[piece_key] >= 3:
                        penalties["structure"] += weight * 1.0

                    if cp_before is not None and cp_before > 200 and move_cpl > 120:
                        penalties["conversion"] += weight * 1.5
                    if cp_before is not None and cp_before > 200 and move_cpl > 200:
                        metrics["advantage_loss"] += 1

                    if classification == "blunder":
                        if last_blunder_ply is not None and ply_index - last_blunder_ply <= 6:
                            metrics["tilt_chain"] += 1
                        last_blunder_ply = ply_index

                    if classification in ["mistake", "blunder"] and (not castled or board.is_check()):
                        metrics["king_exposure"] += 1

                    tags: List[str] = []
                    if time_spent is not None and time_spent < 5:
                        tags.append("IMPULSIVE")
                    if classification in ["mistake", "blunder"]:
                        if board.is_attacked_by(not player_color, move.to_square) and not board.is_attacked_by(player_color, move.to_square):
                            tags.append("HANGING_PIECE")
                    if best_reply:
                        reply_move = chess.Move.from_uci(best_reply)
                        if board.is_capture(reply_move) or board.gives_check(reply_move):
                            tags.append("MISSED_THREAT")
                    if piece and piece.piece_type == chess.KING:
                        tags.append("KING_SAFETY")
                    if not castled and ply_index >= 20:
                        tags.append("KING_SAFETY")
                    if move_cpl >= 200:
                        tags.append("TACTICAL_MISS")
                    if ply_index < 20 and classification in ["mistake", "blunder"]:
                        tags.append("OPENING_PRINCIPLE")
                    if cp_before is not None and cp_before > 200 and move_cpl > 120:
                        tags.append("CONVERSION_ERROR")
                    if classification == "blunder" and last_blunder_ply is not None and ply_index - last_blunder_ply <= 6:
                        tags.append("TILT_CHAIN")

                    move_records.append({
                        "ply": ply_index + 1,
                        "move_number": (ply_index // 2) + 1,
                        "phase": phase,
                        "eval_before": eval_before_player,
                        "eval_after": eval_after_player,
                        "delta": delta,
                        "classification": classification,
                        "tags": tags,
                        "time_spent": time_spent,
                        "opening_name": opening_name,
                        "eco": eco,
                        "confidence": opening_confidence,
                        "move_san": san,
                        "move_uci": move.uci(),
                        "best_move": best_move,
                        "fen_before": fen_before,
                        "fen_after": board.fen(),
                    })

                ply_index += 1
                if ply_index >= 120:
                    break

            if len(minor_developed) < 4 and player_moves > 0:
                penalties["structure"] += weight * (4 - len(minor_developed)) * 0.6
            if not castled and player_moves > 6:
                penalties["king_safety"] += weight * 1.0
            if castled and ply_index >= 20:
                metrics["late_castle"] += 1
            if not castled and player_moves > 6:
                metrics["late_castle"] += 1
            metrics["advantage_positions"] = advantage_positions

            avg_cpl = round(total_cpl / max(1, player_moves), 1)
            accuracy = max(0.0, round(100 - (avg_cpl / 10), 1))

            return {
                "analysis": {
                    "blunders": blunders,
                    "mistakes": mistakes,
                    "inaccuracies": inaccuracies,
                    "avg_cpl": avg_cpl,
                    "accuracy": accuracy,
                },
                "penalties": penalties,
                "phase": phase_stats,
                "moves": player_moves,
                "metrics": metrics,
                "move_records": move_records,
                "opening_name": opening_name,
                "eco": eco,
            }
        finally:
            engine.quit()

    return await run_in_threadpool(_analyze_game)

def calculate_style_scores(
    aggregate: Dict[str, Any],
    axes_scores: Dict[str, float],
    summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    metrics = aggregate.get("metrics", {})
    total_moves = max(1, aggregate.get("moves", 1))
    blunders = (summary or {}).get("blunders", 0)
    mistakes = (summary or {}).get("mistakes", 0)
    avg_accuracy = (summary or {}).get("avg_accuracy", 50.0)

    fast_moves_rate = min(1.0, metrics.get("fast_moves", 0) / total_moves)
    tilt_rate = min(1.0, metrics.get("tilt_chain", 0) / max(1, blunders))
    threat_miss_rate = min(1.0, metrics.get("threat_miss", 0) / max(1, mistakes + blunders))
    king_exposure_rate = min(1.0, metrics.get("king_exposure", 0) / total_moves)

    aggression = round(min(100.0, fast_moves_rate * 60 + king_exposure_rate * 200 + (100 - axes_scores.get("discipline", 50)) * 0.2), 1)
    precision = round(float(avg_accuracy), 1)
    tactics = round(float(axes_scores.get("tactics", 50)), 1)
    strategy = round(float(axes_scores.get("structure", 50)), 1)
    defense = round(float(axes_scores.get("king_safety", 50)), 1)
    chaos = round(min(100.0, tilt_rate * 150 + (100 - axes_scores.get("discipline", 50)) * 0.4 + fast_moves_rate * 30), 1)
    conversion = round(float(axes_scores.get("conversion", 50)), 1)
    exploitation = round(min(100.0, (1.0 - threat_miss_rate) * 80 + axes_scores.get("tactics", 50) * 0.2), 1)

    return {
        "aggression": aggression,
        "precision": precision,
        "tactics": tactics,
        "strategy": strategy,
        "defense": defense,
        "chaos": chaos,
        "conversion": conversion,
        "exploitation": exploitation,
    }


def detect_archetype(style_scores: Dict[str, float]) -> Dict[str, Any]:
    normalized = {k: v / 100.0 for k, v in style_scores.items()}
    best_archetype = PLAYER_ARCHETYPES[0]
    best_score = -1.0

    for archetype in PLAYER_ARCHETYPES:
        score = sum(normalized.get(dim, 0.0) * w for dim, w in archetype["weights"].items())
        if score > best_score:
            best_score = score
            best_archetype = archetype

    return {
        "id": best_archetype["id"],
        "name": best_archetype["name"],
        "description": best_archetype["description"],
        "color": best_archetype["color"],
        "match_score": round(best_score * 100, 1),
    }


def build_player_profile(
    aggregate: Dict[str, Any],
    chesscom_rating: Optional[int],
    summary: Optional[Dict[str, Any]] = None,
    tag_stats: Optional[Dict[str, Any]] = None,
    phase_summary: Optional[Dict[str, Any]] = None,
    opening_stats: Optional[Dict[str, Any]] = None,
    detailed_report: Optional[str] = None,
    short_summary: Optional[str] = None,
    strengths_ai: Optional[List[Dict[str, str]]] = None,
    weaknesses_ai: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    axes_scores = {}
    axis_elos = {}
    for axis, penalty in aggregate["penalties"].items():
        score = score_axis(penalty, aggregate["moves"], factor=0.9)
        axes_scores[axis] = score
        axis_elos[axis] = axis_to_elo(score)

    weaknesses = sorted(axes_scores.items(), key=lambda x: x[1])[:2]
    strengths = sorted(axes_scores.items(), key=lambda x: x[1], reverse=True)[:2]

    training_map = {
        "discipline": {
            "title": "Discipline & anti‑blunder",
            "description": "Objectif: ralentir et sécuriser chaque coup. La majorité des erreurs viennent d’un manque de vérification rapide.",
            "tips": [
                "Pause de 2 secondes avant chaque coup",
                "Regarde captures/échecs adverses en premier",
                "Scan rapide: aucune pièce non défendue",
            ],
        },
        "king_safety": {
            "title": "Sécurité du roi",
            "description": "Objectif: roquer tôt et éviter les faiblesses devant le roi. La sécurité passe avant l’attaque.",
            "tips": [
                "Roque avant le coup 10 dans 8/10 parties",
                "Évite d’avancer f et g sans raison",
                "Active une tour juste après le roque",
            ],
        },
        "tactics": {
            "title": "Vision tactique",
            "description": "Objectif: reconnaître les motifs simples. Quelques schémas reviennent tout le temps.",
            "tips": [
                "Exercices quotidiens: fourchettes, clouages, découvertes",
                "Cherche les échecs forcés avant de jouer",
                "Vérifie les pièces non protégées",
            ],
        },
        "structure": {
            "title": "Structure & développement",
            "description": "Objectif: développer toutes les pièces une fois avant de rejouer la même pièce.",
            "tips": [
                "Deux pièces développées avant le coup 6",
                "Évite les poussées de pions inutiles",
                "Coordonne cavaliers et fous avant l’attaque",
            ],
        },
        "conversion": {
            "title": "Conversion d’avantage",
            "description": "Objectif: gagner sans stress quand tu es mieux. Simplifie et ferme la partie.",
            "tips": [
                "Échange les pièces quand tu es devant",
                "Évite les complications tactiques inutiles",
                "Amène le roi en finale",
            ],
        },
    }

    training_plan = [training_map[w[0]] for w in weaknesses if w[0] in training_map]
    if len(training_plan) < 4:
        training_plan.append({
            "title": "Routine d’ouvertures",
            "description": "Objectif: stabiliser les 10 premiers coups pour éviter les erreurs rapides.",
            "tips": [
                "Répète 2 ouvertures par couleur",
                "Joue 10 minutes de drills par jour",
                "Évite de redéplacer la même pièce",
            ],
        })
    if len(training_plan) < 5:
        training_plan.append({
            "title": "Routine tactique courte",
            "description": "Objectif: garder le cerveau tactique actif chaque jour.",
            "tips": [
                "10 puzzles rapides par jour",
                "Prends 5 secondes pour vérifier les menaces",
                "Note une erreur clé par partie",
            ],
        })

    coach_comment = (
        f"Ton Elo Chess.com est {chesscom_rating or 'N/A'}, mais ton plafond est limité par {weaknesses[0][0]}"
        if weaknesses
        else "On n’a pas assez de données pour un profil complet."
    )

    phase_accuracy = {}
    for phase, data in aggregate["phase"].items():
        avg_phase_cpl = data["cpl"] / max(1, data["moves"])
        phase_accuracy[phase] = max(0.0, round(100 - (avg_phase_cpl / 10), 1))

    style_scores = calculate_style_scores(aggregate, axes_scores, summary)
    archetype = detect_archetype(style_scores)

    return {
        "axes": axes_scores,
        "axis_elos": axis_elos,
        "weaknesses": [w[0] for w in weaknesses],
        "strengths": [s[0] for s in strengths],
        "training_plan": training_plan,
        "coach_comment": short_summary or coach_comment,
        "performance_by_phase": phase_accuracy,
        "metrics": aggregate.get("metrics", {}),
        "summary": summary or {},
        "tag_stats": tag_stats or {},
        "phase_summary": phase_summary or {},
        "opening_stats": opening_stats or {},
        "detailed_report": detailed_report or "",
        "short_summary": short_summary or "",
        "strengths_ai": strengths_ai or [],
        "weaknesses_ai": weaknesses_ai or [],
        "style_scores": style_scores,
        "archetype": archetype,
    }

@api_router.get("/user/usage/{user_id}")
async def get_user_usage(user_id: str):
    usage = await db.user_usage.find_one({"user_id": user_id}, {"_id": 0})
    if not usage:
        return {
            "imports_count": 0,
            "is_premium": False,
            "limit": FREE_MAX_ANALYSES,
            "max_games": FREE_MAX_GAMES,
            "premium_enabled": PREMIUM_ENABLED,
        }
    max_games = PREMIUM_MAX_GAMES if usage.get("is_premium") else FREE_MAX_GAMES
    limit = None if usage.get("is_premium") else FREE_MAX_ANALYSES
    return {
        "imports_count": usage["imports_count"],
        "is_premium": usage.get("is_premium", False),
        "limit": limit,
        "max_games": max_games,
        "premium_enabled": PREMIUM_ENABLED,
    }


@api_router.get("/chessdotcom/stats/{username}")
async def get_chesscom_stats(username: str):
    stats_data = await fetch_chesscom_json(
        f"https://api.chess.com/pub/player/{username.lower()}/stats"
    )

    ratings = [
        stats_data.get("chess_rapid", {}).get("last", {}).get("rating"),
        stats_data.get("chess_blitz", {}).get("last", {}).get("rating"),
        stats_data.get("chess_bullet", {}).get("last", {}).get("rating"),
    ]
    ratings = [rating for rating in ratings if rating is not None]
    max_rating = max(ratings) if ratings else None

    return {"max_rating": max_rating, "stats": stats_data}

@api_router.post("/coach/evaluate-move")
async def evaluate_move(payload: MoveEvalRequest):
    player_is_white = payload.player_color.lower() == "white"

    # Fast-path: if the position after the move is already checkmate, it's a brilliant move.
    # We check this with python-chess directly — no need to call Stockfish on a dead position.
    try:
        _board_after = chess.Board(payload.fen_after)
        if _board_after.is_checkmate():
            eval_before = await analyze_position_stockfish(payload.fen_before)
            return {
                "cp_before": 700,
                "cp_after": 700,
                "cpl": 0.0,
                "classification": "brilliant",
                "accuracy": 100.0,
                "best_move": eval_before.get("best_move"),
                "mate_before": eval_before.get("mate"),
            }
    except Exception:
        pass  # Invalid FEN — fall through to normal analysis

    eval_before = await analyze_position_stockfish(payload.fen_before)
    eval_after = await analyze_position_stockfish(payload.fen_after)

    cp_before = eval_before.get("cp")
    cp_after = eval_after.get("cp")

    # Cap mate scores to avoid false blunder classification.
    # Without this, missing a forced mate (e.g. +10000 → +500) generates 9500cp "blunder".
    LIVE_MAT_CAP = 700
    if cp_before is not None:
        cp_before = max(-LIVE_MAT_CAP, min(LIVE_MAT_CAP, cp_before))
    if cp_after is not None:
        cp_after = max(-LIVE_MAT_CAP, min(LIVE_MAT_CAP, cp_after))

    cpl = 0.0
    if cp_before is not None and cp_after is not None:
        if player_is_white:
            cpl = max(0.0, cp_before - cp_after)
        else:
            cpl = max(0.0, cp_after - cp_before)

    # Use lenient live thresholds (shallow 80ms analysis can swing wildly)
    classification = classify_move_cpl_live(cpl)

    # Anti-false-positive guard: shallow analysis can give wildly inconsistent evals
    # for forcing moves (checks, attacks). If the player's position AFTER the move is
    # still non-negative (equal or winning), it CANNOT be a blunder or mistake.
    # Example: Rb1+ looks like -400 cpl at depth 8 but White is still +3.
    if cp_after is not None and classification in ('blunder', 'mistake'):
        player_cp_after = cp_after if player_is_white else -cp_after
        if player_cp_after >= -50:  # Player is still equal or better → downgrade
            classification = 'ok'

    accuracy = max(0.0, round(100 - (cpl / 10), 1))

    return {
        "cp_before": cp_before,
        "cp_after": cp_after,
        "cpl": round(cpl, 1),
        "classification": classification,
        "accuracy": accuracy,
        "best_move": eval_before.get("best_move"),
        "mate_before": eval_before.get("mate"),
    }

class AnalyzeGameRequest(BaseModel):
    pgn: str
    player_color: str = "white"  # "white" or "black"
    username: Optional[str] = None

@api_router.post("/analyze-game")
async def analyze_game_endpoint(payload: AnalyzeGameRequest):
    """Analyze every move in a PGN. Returns list of annotated moves with
    stockfish eval + AI commentary for key moments (blunder/mistake)."""
    from chess.pgn import read_game
    import io as _io

    pgn_io = _io.StringIO(payload.pgn)
    game_obj = read_game(pgn_io)
    if not game_obj:
        raise HTTPException(status_code=400, detail="PGN invalide")

    board = game_obj.board()
    player_is_white = payload.player_color.lower() == "white"
    moves_data = []
    MAT_CAP = 700

    for node in game_obj.mainline():
        fen_before = board.fen()
        move = node.move
        san = board.san(move)
        board.push(move)
        fen_after = board.fen()

        # Stockfish eval (fast, import depth)
        eval_before = await analyze_position_stockfish(fen_before)
        eval_after = await analyze_position_stockfish(fen_after)

        cp_b = eval_before.get("cp")
        cp_a = eval_after.get("cp")
        if cp_b is not None:
            cp_b = max(-MAT_CAP, min(MAT_CAP, cp_b))
        if cp_a is not None:
            cp_a = max(-MAT_CAP, min(MAT_CAP, cp_a))

        # Determine whose move it was (before push, it was board.turn)
        move_is_white = not board.turn  # after push, turn flipped
        is_player_move = (move_is_white == player_is_white)

        cpl = 0.0
        if cp_b is not None and cp_a is not None:
            if move_is_white:
                cpl = max(0.0, cp_b - cp_a)
            else:
                cpl = max(0.0, cp_a - cp_b)

        classification = classify_move_cpl(cpl) if is_player_move else "opponent"
        best_move = eval_before.get("best_move")

        moves_data.append({
            "san": san,
            "fen_after": fen_after,
            "cp_before": cp_b,
            "cp_after": cp_a,
            "cpl": round(cpl, 1),
            "classification": classification,
            "is_player_move": is_player_move,
            "best_move": best_move,
            "comment": None,  # filled below for key moments
        })

    # ─── Coach Rasta system prompt — sérieux, analytique, style chess.com ──────
    RASTA_SYSTEM = (
        "Tu es Coach Rasta, coach d'échecs professionnel et analyste. "
        "Ton style : sérieux, précis, pédagogique. Tu analyses chaque position avec rigueur, "
        "comme un commentateur professionnel (style Kasparov Academy ou analyse chess.com). "
        "Tu identifies clairement : la structure de pions, l'activité des pièces, les plans tactiques disponibles, "
        "les erreurs stratégiques. Tu relies chaque erreur à un principe concret d'échecs. "
        "Quand le profil du joueur est fourni, tu références explicitement ses statistiques "
        "(précision, faiblesses récurrentes, rating) pour personnaliser le diagnostic. "
        "Format commentaire coup : 2-3 phrases max. Commence par décrire la position, "
        "puis explique pourquoi le coup est problématique, puis donne le plan correct. "
        "Pas de 'street speak', pas de familiarité excessive — coach professionnel et exigeant. "
        "Langue : français uniquement. Sois direct et factuel."
    )

    # ─── Fetch player profile for contextual analysis ─────────────────────────
    player_stats_context = ""
    if payload.username:
        try:
            username_lower = payload.username.lower()
            profile = await db.player_profiles.find_one(
                {"$or": [{"username": username_lower}, {"chesscom_username": username_lower}]},
                {"_id": 0}
            )
            if profile:
                wks = profile.get("weaknesses", [])
                acc  = profile.get("avg_accuracy", 0)
                blun = profile.get("avg_blunders_per_game", "?")
                mist = profile.get("avg_mistakes_per_game", "?")
                wr   = round((profile.get("winrate") or 0) * 100, 1)
                elo  = profile.get("chesscom_rating", "?")
                strengths = profile.get("strengths_ai", [])
                player_stats_context = (
                    f"\n\nPROFIL HISTORIQUE DU JOUEUR (sur ses dernières parties analysées) :\n"
                    f"- Rating Chess.com : {elo}\n"
                    f"- Précision moyenne : {round(float(acc), 1)}%\n"
                    f"- Gaffes par partie : {blun}\n"
                    f"- Erreurs par partie : {mist}\n"
                    f"- Winrate : {wr}%\n"
                    f"- Faiblesses récurrentes identifiées : {', '.join(wks) if wks else 'non encore identifiées'}\n"
                    f"- Points forts : {', '.join([s.get('title','') for s in strengths]) if strengths else 'non identifiés'}\n"
                    f"IMPORTANT : réfère-toi explicitement à ces stats dans ton analyse pour personnaliser le diagnostic."
                )
        except Exception:
            pass

    # ─── Build stats for global analysis ──────────────────────────────────────
    total_player_moves = sum(1 for m in moves_data if m["is_player_move"])
    blunder_list = ", ".join(
        f"{m['san']} (coup {i//2+1})"
        for i, m in enumerate(moves_data)
        if m["is_player_move"] and m["classification"] == "blunder"
    ) or "aucune"
    mistake_list = ", ".join(
        f"{m['san']} (coup {i//2+1})"
        for i, m in enumerate(moves_data)
        if m["is_player_move"] and m["classification"] == "mistake"
    ) or "aucune"
    best_list = ", ".join(
        f"{m['san']} (coup {i//2+1})"
        for i, m in enumerate(moves_data)
        if m["is_player_move"] and m["classification"] == "best"
    ) or "aucun"

    # ─── Per-move AI comments (blunders + mistakes + inaccuracies, max 5) ──────
    # Priority: blunders first, then mistakes, then inaccuracies
    ai_client = get_openai_client()  # separate from MongoDB 'client' variable

    key_indices = sorted(
        [
            i for i, m in enumerate(moves_data)
            if m["is_player_move"] and m["classification"] in ("blunder", "mistake", "inaccuracy")
        ],
        key=lambda i: {"blunder": 0, "mistake": 1, "inaccuracy": 2}[moves_data[i]["classification"]]
    )[:5]

    if ai_client and key_indices:
        def _gen_comments():
            comments = {}
            fens = [moves_data[i]["fen_after"] for i in range(len(moves_data))]
            START = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"
            for idx in key_indices:
                m = moves_data[idx]
                fen_before = fens[idx - 1] if idx > 0 else START
                move_num = idx // 2 + 1
                prompt = (
                    f"Coup {move_num} joué : {m['san']} "
                    f"({m['classification']}, {round(float(m['cpl']), 0)} centipions perdus).\n"
                    f"Meilleur coup : {m['best_move'] or 'inconnu'}.\n"
                    f"Position FEN avant le coup : {fen_before}"
                    f"{player_stats_context}\n\n"
                    f"En 2-3 phrases : analyse la position, explique pourquoi ce coup est "
                    f"{m['classification']}, et relie à une habitude du joueur si pertinent. "
                    f"Style Coach Rasta."
                )
                try:
                    resp = ai_client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": RASTA_SYSTEM},
                            {"role": "user", "content": prompt},
                        ],
                        max_tokens=130,
                        temperature=0.85,
                    )
                    comments[idx] = resp.choices[0].message.content.strip()
                except Exception:
                    comments[idx] = None
            return comments

        loop = asyncio.get_event_loop()
        comments_map = await loop.run_in_executor(None, _gen_comments)
        for idx, comment in comments_map.items():
            moves_data[idx]["comment"] = comment

    # ─── Global game analysis ─────────────────────────────────────────────────
    global_analysis = None
    if ai_client and len(moves_data) > 0:
        # Calcul précision approx (meilleur-coup = 0 cpl, blunder = cpl élevé)
        player_moves = [m for m in moves_data if m["is_player_move"]]
        avg_cpl = round(sum(m["cpl"] for m in player_moves) / max(1, len(player_moves)), 1)
        best_pct = round(sum(1 for m in player_moves if m["classification"] == "best") / max(1, len(player_moves)) * 100)
        blunder_count = sum(1 for m in player_moves if m["classification"] == "blunder")
        mistake_count = sum(1 for m in player_moves if m["classification"] == "mistake")

        def _gen_global():
            prompt = (
                f"Analyse complète d'une partie d'échecs — style analyse professionnelle chess.com.\n"
                f"Couleur du joueur : {payload.player_color}\n"
                f"Total coups : {len(moves_data)} | Coups du joueur : {total_player_moves}\n"
                f"Stats de cette partie : {blunder_count} gaffe(s), {mistake_count} erreur(s), "
                f"CPL moyen : {avg_cpl}, {best_pct}% de coups excellents\n"
                f"Gaffes : {blunder_list}\n"
                f"Erreurs : {mistake_list}\n"
                f"Excellents coups : {best_list}"
                f"{player_stats_context}\n\n"
                f"Rédige une analyse structurée en 5 sections (style chess.com Game Review) :\n"
                f"📊 Résumé : CPL moyen, précision estimée, verdict global (1-2 phrases)\n"
                f"♟️ Ouverture : évaluation de la phase d'ouverture (bon/mauvais développement, structure)\n"
                f"⚔️ Moments clés : les 2-3 coups les plus décisifs avec explication tactique/stratégique\n"
                f"🔍 Tendances du joueur : compare cette partie aux stats historiques si disponibles "
                f"(est-ce que les faiblesses récurrentes se manifestent ici ?)\n"
                f"📈 Plan de progrès : 2-3 axes concrets (thèmes tactiques à travailler, "
                f"principes stratégiques à appliquer)\n\n"
                f"Sois précis et factuel. 200 mots max. Français uniquement."
            )
            try:
                resp = ai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": RASTA_SYSTEM},
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=400,
                    temperature=0.85,
                )
                return resp.choices[0].message.content.strip()
            except Exception:
                return None

        loop = asyncio.get_event_loop()
        global_analysis = await loop.run_in_executor(None, _gen_global)

    return {"moves": moves_data, "total": len(moves_data), "global_analysis": global_analysis}


@api_router.post("/coach/move")
async def coach_move(payload: CoachMoveRequest):
    engine = await stockfish_pool.get()
    try:
        def _compute():
            if payload.elo >= 1400:
                try:
                    engine.configure({"UCI_LimitStrength": False, "Skill Level": 20})
                except Exception:
                    pass
            else:
                skill = max(1, min(20, int((payload.elo - 200) / 70)))
                try:
                    engine.configure({"UCI_LimitStrength": True, "UCI_Elo": payload.elo, "Skill Level": skill})
                except Exception:
                    pass
            board = chess.Board(payload.fen)
            limit = chess.engine.Limit(time=0.4 if payload.elo >= 1400 else 0.25)
            info = engine.analyse(board, limit, multipv=3)
            candidates = info if isinstance(info, list) else [info]

            def score_info(entry):
                return entry["score"].white().score(mate_score=10000) or -10000

            scored = []
            for entry in candidates:
                move = entry.get("pv", [None])[0]
                if move is None:
                    continue
                score = score_info(entry)
                adjusted = score
                if payload.style == "offensif" and (board.is_capture(move) or board.gives_check(move)):
                    adjusted += 35
                if payload.style == "defensif" and (board.is_castling(move) or board.is_check()):
                    adjusted += 35
                if payload.style == "positionnel" and not (
                    board.is_capture(move) or board.gives_check(move) or board.is_castling(move)
                ):
                    adjusted += 25
                scored.append((adjusted, move))

            if not scored:
                move = random.choice(list(board.legal_moves))
                return move.uci()

            scored.sort(key=lambda x: x[0], reverse=True)
            error_rate = get_elo_error_rate(payload.elo)
            if random.random() < error_rate and len(scored) > 1:
                pick_pool = scored[1: min(4, len(scored))]
                return random.choice(pick_pool)[1].uci()

            return scored[0][1].uci()

        move = await run_in_threadpool(_compute)
    finally:
        # Réinitialiser les options UCI avant de remettre dans le pool
        try:
            engine.configure({"UCI_LimitStrength": False, "Skill Level": 20})
        except Exception:
            pass
        await stockfish_pool.put(engine)

    return {"uci": move}

@api_router.post("/import/chessdotcom/{username}")
async def import_chesscom_games(username: str, payload: Optional[ChessComImportRequest] = None):
    if payload is None:
        payload = ChessComImportRequest(user_id=username)
    if not payload.user_id:
        raise HTTPException(status_code=400, detail="user_id requis")

    user_id = payload.user_id

    # Premium gating — only active when PREMIUM_ENABLED=true
    if PREMIUM_ENABLED and user_id not in BYPASS_PREMIUM_USER_IDS:
        usage = await db.user_usage.find_one({"user_id": user_id}) or {"imports_count": 0, "is_premium": False}
        if not usage.get("is_premium") and usage["imports_count"] >= FREE_MAX_ANALYSES:
            raise HTTPException(status_code=403, detail={
                "code": "LIMIT_REACHED",
                "message": "Limite de 3 analyses gratuites atteinte",
                "imports_count": usage["imports_count"],
                "limit": FREE_MAX_ANALYSES,
            })
        if not usage.get("is_premium"):
            payload.max_games = FREE_MAX_GAMES

    months = payload.months if payload.months and payload.months > 0 else 3
    months = min(12, months)
    username_clean = username.lower().strip()

    # Validate username: Chess.com allows letters, digits, underscores, hyphens, 3-25 chars
    import re as _re
    if not _re.match(r'^[a-z0-9_\-]{3,25}$', username_clean):
        raise HTTPException(status_code=422, detail="Nom d'utilisateur Chess.com invalide (3-25 caractères, lettres/chiffres/_/-)")

    archives_url = f"https://api.chess.com/pub/player/{username_clean}/games/archives"
    archives_data = await fetch_chesscom_json(archives_url)
    archives = archives_data.get("archives", [])

    if not archives:
        await db.chesscom_games.delete_many({"user_id": payload.user_id, "username": username_clean})
        return {"imported": 0, "games": [], "months": months}

    selected_archives = archives[-months:]
    raw_games: List[Dict[str, Any]] = []
    for archive_url in selected_archives:
        archive_data = await fetch_chesscom_json(archive_url)
        raw_games.extend(archive_data.get("games", []))

    max_games = payload.max_games if payload.max_games and payload.max_games > 0 else 10
    raw_games = sorted(raw_games, key=lambda g: g.get("end_time", 0), reverse=True)[:max_games]

    games: List[ChessComImportedGame] = []
    analysis_summary = {
        "games_analyzed": 0,
        "blunders": 0,
        "mistakes": 0,
        "inaccuracies": 0,
        "total_accuracy": 0.0,
        "avg_accuracy": 0.0,
    }

    aggregate = {
        "penalties": {"discipline": 0.0, "king_safety": 0.0, "tactics": 0.0, "structure": 0.0, "conversion": 0.0},
        "moves": 0,
        "phase": {"opening": {"cpl": 0.0, "moves": 0}, "middlegame": {"cpl": 0.0, "moves": 0}, "endgame": {"cpl": 0.0, "moves": 0}},
        "metrics": {
            "fast_moves": 0,
            "fast_blunders": 0,
            "threat_miss": 0,
            "hanging_blunders": 0,
            "late_castle": 0,
            "king_exposure": 0,
            "advantage_loss": 0,
            "tilt_chain": 0,
            "advantage_positions": 0,
        },
    }
    all_move_records: List[Dict[str, Any]] = []
    game_stats: List[Dict[str, Any]] = []

    player_ratings = []

    for game in raw_games:
        white = game.get("white", {}) or {}
        black = game.get("black", {}) or {}
        pgn_text = game.get("pgn", "")

        outcome = get_game_outcome(pgn_text, username_clean)
        time_class = game.get("time_class", "")
        end_time = game.get("end_time", 0)
        rated = bool(game.get("rated", False))

        white_username = str(white.get("username", ""))
        black_username = str(black.get("username", ""))
        player_is_white = white_username.lower() == username_clean
        opponent_rating = black.get("rating") if player_is_white else white.get("rating")
        player_rating = white.get("rating") if player_is_white else black.get("rating")
        if player_rating:
            player_ratings.append(player_rating)

        white_result = str(white.get("result", ""))
        black_result = str(black.get("result", ""))
        result = "1/2-1/2"
        if white_result == "win":
            result = "1-0"
        elif black_result == "win":
            result = "0-1"

        analysis_result = None
        if pgn_text:
            analysis_result = await analyze_game_stockfish(
                pgn_text,
                username_clean,
                time_class,
                end_time,
                opponent_rating,
                result,
                time_limit=STOCKFISH_TIME_LIMIT_IMPORT,
                depth=STOCKFISH_DEPTH_IMPORT,
                max_plies=60,
            )

        analysis = analysis_result["analysis"] if analysis_result else None
        if analysis_result and analysis_result["analysis"]:
            analysis_summary["games_analyzed"] += 1
            analysis_summary["blunders"] += analysis["blunders"]
            analysis_summary["mistakes"] += analysis["mistakes"]
            analysis_summary["inaccuracies"] += analysis["inaccuracies"]
            analysis_summary["total_accuracy"] += analysis.get("accuracy", 0.0)

            aggregate["moves"] += analysis_result["moves"]
            for axis in aggregate["penalties"]:
                aggregate["penalties"][axis] += analysis_result["penalties"].get(axis, 0.0)
            for phase in aggregate["phase"]:
                aggregate["phase"][phase]["cpl"] += analysis_result["phase"][phase]["cpl"]
                aggregate["phase"][phase]["moves"] += analysis_result["phase"][phase]["moves"]
            if analysis_result.get("metrics"):
                for key in aggregate["metrics"]:
                    aggregate["metrics"][key] += analysis_result["metrics"].get(key, 0)

            move_records = analysis_result.get("move_records", [])
            if move_records:
                game_id = game.get("url", "") or f"{username_clean}-{end_time}"
                timestamp = datetime.utcfromtimestamp(end_time) if end_time else datetime.utcnow()
                for record in move_records:
                    record.update({
                        "user_id": payload.user_id,
                        "source": "chesscom",
                        "username": username_clean,
                        "game_id": game_id,
                        "timestamp": timestamp,
                    })
                all_move_records.extend(move_records)

            game_stats.append({
                "game_id": game.get("url", "") or f"{username_clean}-{end_time}",
                "opening_name": analysis_result.get("opening_name"),
                "eco": analysis_result.get("eco"),
                "accuracy": analysis.get("accuracy", 0),
                "result": result,
                "outcome": outcome,
                "end_time": end_time,
            })

        game_obj = ChessComImportedGame(
            user_id=payload.user_id,
            username=username_clean,
            url=game.get("url", ""),
            pgn=pgn_text,
            time_class=time_class,
            rated=rated,
            end_time=end_time,
            white=ChessComPlayer(
                username=white_username,
                rating=white.get("rating"),
                result=str(white.get("result", "")),
            ),
            black=ChessComPlayer(
                username=black_username,
                rating=black.get("rating"),
                result=str(black.get("result", "")),
            ),
            analysis=analysis,
        )
        games.append(game_obj)

    await db.chesscom_games.delete_many({"user_id": payload.user_id, "username": username_clean})
    if games:
        await db.chesscom_games.insert_many([g.model_dump() for g in games])

    games_sorted = sorted(games, key=lambda g: g.end_time, reverse=True)
    summary_games = []
    for game in games_sorted[:200]:
        data = game.model_dump()
        data.pop("pgn", None)
        summary_games.append(data)

    if analysis_summary["games_analyzed"] > 0:
        analysis_summary["avg_accuracy"] = round(
            analysis_summary["total_accuracy"] / analysis_summary["games_analyzed"],
            1,
        )

    metrics = aggregate["metrics"]
    analysis_summary.update({
        "fast_moves": metrics["fast_moves"],
        "fast_blunders": metrics["fast_blunders"],
        "threat_miss": metrics["threat_miss"],
        "hanging_blunders": metrics["hanging_blunders"],
        "late_castle": metrics["late_castle"],
        "king_exposure": metrics["king_exposure"],
        "advantage_loss": metrics["advantage_loss"],
        "tilt_chain": metrics["tilt_chain"],
        "fast_blunder_rate": round(metrics["fast_blunders"] / max(1, analysis_summary["blunders"]), 2),
        "threat_miss_rate": round(metrics["threat_miss"] / max(1, analysis_summary["mistakes"] + analysis_summary["blunders"]), 2),
        "advantage_loss_rate": round(metrics["advantage_loss"] / max(1, metrics["advantage_positions"]), 2),
    })

    if all_move_records:
        await db.move_analysis.delete_many({"user_id": payload.user_id, "username": username_clean, "source": "chesscom"})
        await db.move_analysis.insert_many(all_move_records)

    game_stats_sorted = sorted(game_stats, key=lambda g: g.get("end_time") or 0, reverse=True)
    recent_game_ids = {g["game_id"] for g in game_stats_sorted[:10]}
    previous_game_ids = {g["game_id"] for g in game_stats_sorted[10:20]}

    summary = {
        "games_played": len(game_stats),
        "winrate": round(sum(1 for g in game_stats if g.get("outcome") == "win") / max(1, len(game_stats)), 3),
        "avg_accuracy": analysis_summary.get("avg_accuracy", 0),
        "avg_blunders_per_game": round(analysis_summary["blunders"] / max(1, analysis_summary["games_analyzed"]), 2),
        "avg_mistakes_per_game": round(analysis_summary["mistakes"] / max(1, analysis_summary["games_analyzed"]), 2),
        "accuracy_trend": 0,
        "fast_blunder_rate": analysis_summary.get("fast_blunder_rate"),
        "threat_miss_rate": analysis_summary.get("threat_miss_rate"),
        "advantage_loss_rate": analysis_summary.get("advantage_loss_rate"),
    }

    if game_stats_sorted:
        recent_acc = [g.get("accuracy", 0) for g in game_stats_sorted[:10]]
        previous_acc = [g.get("accuracy", 0) for g in game_stats_sorted[10:20]]
        if recent_acc and previous_acc:
            summary["accuracy_trend"] = round((sum(recent_acc) / len(recent_acc)) - (sum(previous_acc) / len(previous_acc)), 1)
        if recent_acc:
            summary["recent_accuracy"] = round(sum(recent_acc) / len(recent_acc), 1)

    tag_stats = compute_tag_stats(all_move_records, recent_game_ids, previous_game_ids) if all_move_records else {}
    phase_summary = compute_phase_summary(all_move_records) if all_move_records else {}
    opening_stats = compute_opening_stats(game_stats_sorted, all_move_records) if all_move_records else {}
    detailed_report = build_detailed_report(summary, phase_summary, tag_stats, opening_stats) if all_move_records else ""

    ai_report = {}
    if all_move_records:
        top_tags = sorted(tag_stats.items(), key=lambda x: x[1].get("severity", 0), reverse=True)[:5]
        top_openings = sorted(opening_stats.items(), key=lambda x: x[1].get("games", 0), reverse=True)[:5]
        ai_report = await generate_ai_report({
            "summary": summary,
            "phase_summary": phase_summary,
            "top_tags": top_tags,
            "top_openings": top_openings,
        })
    local_report = build_local_ai_report(summary, phase_summary, tag_stats, opening_stats) if all_move_records else {}
    final_report = ai_report if ai_report else local_report
    ai_detailed = final_report.get("detailed_report") if isinstance(final_report, dict) else None
    ai_short = final_report.get("short_summary") if isinstance(final_report, dict) else None
    strengths_ai = final_report.get("strengths") if isinstance(final_report, dict) else None
    weaknesses_ai = final_report.get("weaknesses") if isinstance(final_report, dict) else None

    last10_records = [m for m in all_move_records if m.get("game_id") in recent_game_ids]
    puzzle_pack = await generate_puzzle_pack(last10_records, summary.get("recent_accuracy", summary.get("avg_accuracy", 0))) if last10_records else {"summary": "", "puzzles": []}

    chesscom_rating = max(player_ratings) if player_ratings else None
    profile = (
        build_player_profile(
            aggregate,
            chesscom_rating,
            summary=summary,
            tag_stats=tag_stats,
            phase_summary=phase_summary,
            opening_stats=opening_stats,
            detailed_report=ai_detailed or detailed_report,
            short_summary=ai_short,
            strengths_ai=strengths_ai,
            weaknesses_ai=weaknesses_ai,
        )
        if analysis_summary["games_analyzed"] > 0
        else None
    )
    if profile:
        await db.player_profiles.update_one(
            {"user_id": payload.user_id, "username": username_clean},
            {"$set": {**profile, "updated_at": datetime.utcnow()}},
            upsert=True,
        )

    # Increment usage counter after successful import
    if PREMIUM_ENABLED and user_id not in BYPASS_PREMIUM_USER_IDS:
        await db.user_usage.update_one(
            {"user_id": user_id},
            {
                "$inc": {"imports_count": 1},
                "$set": {"updated_at": datetime.utcnow()},
                "$setOnInsert": {"is_premium": False, "created_at": datetime.utcnow()},
            },
            upsert=True,
        )

    result_payload = {
        "imported": len(games),
        "games": summary_games,
        "months": months,
        "analysis_summary": analysis_summary,
        "profile": profile,
        "puzzle_pack": puzzle_pack,
    }
    return JSONResponse(content=jsonable_encoder(result_payload, custom_encoder={ObjectId: str}))

@api_router.get("/chessdotcom/games")
async def get_chesscom_games(user_id: str, username: Optional[str] = None, limit: int = 200):
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id requis")

    query: Dict[str, Any] = {"user_id": user_id}
    if username:
        query["username"] = username.lower()

    games = await db.chesscom_games.find(query, {"_id": 0}).sort("end_time", -1).limit(limit).to_list(limit)
    return {"games": games}

@api_router.get("/chessdotcom/profile")
async def get_chesscom_profile(user_id: str, username: str):
    if not user_id or not username:
        raise HTTPException(status_code=400, detail="user_id et username requis")
    profile = await db.player_profiles.find_one(
        {"user_id": user_id, "username": username.lower()},
        {"_id": 0}
    )
    if not profile:
        return {"profile": None}
    return {"profile": profile}

# Helper function to update opening stats
async def update_opening_stats(drill: DrillAttempt):
    stats_key = f"opening_stats.{drill.opening_id}"
    
    # Get current stats
    current = await db.player_stats.find_one({"id": "global"})
    if current and drill.opening_id in current.get("opening_stats", {}):
        current_stats = current["opening_stats"][drill.opening_id]
        new_attempts = current_stats.get("attempts", 0) + 1
        new_completions = current_stats.get("completions", 0) + (1 if drill.completed else 0)
        new_correct = current_stats.get("total_correct", 0) + drill.correct_moves
        new_errors = current_stats.get("total_errors", 0) + drill.errors
        total_moves = new_correct + new_errors
        mastery = (new_correct / total_moves * 100) if total_moves > 0 else 0
    else:
        new_attempts = 1
        new_completions = 1 if drill.completed else 0
        new_correct = drill.correct_moves
        new_errors = drill.errors
        total_moves = drill.correct_moves + drill.errors
        mastery = (drill.correct_moves / total_moves * 100) if total_moves > 0 else 0
    
    await db.player_stats.update_one(
        {"id": "global"},
        {"$set": {
            stats_key: {
                "opening_id": drill.opening_id,
                "opening_name": drill.opening_name,
                "attempts": new_attempts,
                "completions": new_completions,
                "total_correct": new_correct,
                "total_errors": new_errors,
                "mastery_level": round(mastery, 1)
            },
            "updated_at": datetime.utcnow()
        }},
        upsert=True
    )

# Openings data route
@api_router.get("/openings")
async def get_openings():
    """Return the list of available openings"""
    # This would typically come from a database, but for MVP we return hardcoded data
    return {
        "openings": [
            {"id": "italian-game", "eco": "C50", "name": "Partie Italienne", "color": "white", "difficulty": "débutant"},
            {"id": "ruy-lopez", "eco": "C60", "name": "Partie Espagnole", "color": "white", "difficulty": "intermédiaire"},
            {"id": "london-system", "eco": "D02", "name": "Système de Londres", "color": "white", "difficulty": "débutant"},
            {"id": "queens-gambit", "eco": "D06", "name": "Gambit Dame", "color": "white", "difficulty": "intermédiaire"},
            {"id": "scotch-game", "eco": "C45", "name": "Partie Écossaise", "color": "white", "difficulty": "débutant"},
            {"id": "english-opening", "eco": "A20", "name": "Ouverture Anglaise", "color": "white", "difficulty": "avancé"},
            {"id": "vienna-game", "eco": "C25", "name": "Partie Viennoise", "color": "white", "difficulty": "intermédiaire"},
            {"id": "kings-indian-attack", "eco": "A07", "name": "Attaque Indienne du Roi", "color": "white", "difficulty": "débutant"},
            {"id": "sicilian-defense", "eco": "B20", "name": "Défense Sicilienne", "color": "black", "difficulty": "intermédiaire"},
            {"id": "french-defense", "eco": "C00", "name": "Défense Française", "color": "black", "difficulty": "intermédiaire"},
            {"id": "caro-kann", "eco": "B10", "name": "Défense Caro-Kann", "color": "black", "difficulty": "débutant"},
            {"id": "kings-indian-defense", "eco": "E60", "name": "Défense Indienne du Roi", "color": "black", "difficulty": "avancé"},
            {"id": "nimzo-indian", "eco": "E20", "name": "Défense Nimzo-Indienne", "color": "black", "difficulty": "avancé"},
            {"id": "slav-defense", "eco": "D10", "name": "Défense Slave", "color": "black", "difficulty": "intermédiaire"},
            {"id": "queens-gambit-declined", "eco": "D30", "name": "Gambit Dame Décliné", "color": "black", "difficulty": "intermédiaire"},
            {"id": "scandinavian-defense", "eco": "B01", "name": "Défense Scandinave", "color": "black", "difficulty": "débutant"},
            {"id": "pirc-defense", "eco": "B07", "name": "Défense Pirc", "color": "black", "difficulty": "intermédiaire"},
        ]
    }

# --------------------------------------------------------------------------- #
# RevenueCat webhook — called by RevenueCat when a purchase/renewal happens   #
# --------------------------------------------------------------------------- #
REVENUECAT_WEBHOOK_SECRET = os.getenv("REVENUECAT_WEBHOOK_SECRET", "")

# Event types that grant premium access
_RC_PREMIUM_EVENTS = {
    "INITIAL_PURCHASE",
    "RENEWAL",
    "PRODUCT_CHANGE",
    "UNCANCELLATION",
    "SUBSCRIBER_ALIAS",
    "NON_SUBSCRIPTION_PURCHASE",
}

# Event types that revoke premium access
_RC_REVOKE_EVENTS = {
    "EXPIRATION",
    "CANCELLATION",
    "BILLING_ISSUE",
}

@api_router.post("/webhook/revenuecat")
async def revenuecat_webhook(request: Request):
    """
    Receives RevenueCat events and updates the user's is_premium flag in MongoDB.

    Configure in RevenueCat dashboard:
      URL: https://<your-domain>/api/webhook/revenuecat
      Authorization header: Bearer <REVENUECAT_WEBHOOK_SECRET>
    """
    # Validate authorization header (if secret is configured)
    if REVENUECAT_WEBHOOK_SECRET:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != f"Bearer {REVENUECAT_WEBHOOK_SECRET}":
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    body = await request.json()
    event = body.get("event", {})
    event_type = event.get("type", "")
    # RevenueCat sends app_user_id as the user identifier
    app_user_id = event.get("app_user_id") or event.get("original_app_user_id")

    if not app_user_id:
        return {"status": "ignored", "reason": "no app_user_id"}

    if event_type in _RC_PREMIUM_EVENTS:
        await db.user_usage.update_one(
            {"user_id": app_user_id},
            {
                "$set": {"is_premium": True, "premium_since": datetime.utcnow()},
                "$setOnInsert": {"user_id": app_user_id, "imports_count": 0, "created_at": datetime.utcnow()},
            },
            upsert=True,
        )
        logger.info(f"[RevenueCat] Premium granted: {app_user_id} ({event_type})")
        return {"status": "premium_granted", "user_id": app_user_id}

    if event_type in _RC_REVOKE_EVENTS:
        await db.user_usage.update_one(
            {"user_id": app_user_id},
            {"$set": {"is_premium": False}},
        )
        logger.info(f"[RevenueCat] Premium revoked: {app_user_id} ({event_type})")
        return {"status": "premium_revoked", "user_id": app_user_id}

    return {"status": "ignored", "event_type": event_type}


# Include the router in the main app
app.include_router(api_router)

# CORS — for a mobile app the requests come from the device (not a browser),
# so wildcard origins are safe. allow_credentials=False is required with allow_origins=["*"].
# To restrict further (e.g. for a future web app), set ALLOWED_ORIGINS=https://yourdomain.com on Railway.
_raw_origins = os.getenv("ALLOWED_ORIGINS", "*")
_allowed_origins = [o.strip() for o in _raw_origins.split(",")] if _raw_origins != "*" else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_credentials=False,
    allow_origins=_allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
