
import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from typing import List

INDEX_URL = "https://raw.githubusercontent.com/ondata/dati_catastali/main/S_0000_ITALIA/anagrafica/index.parquet"
BASE_URL  = "https://raw.githubusercontent.com/ondata/dati_catastali/main/S_0000_ITALIA/anagrafica/"

app = FastAPI(title="Catasto Lookup API", version="1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Connessione DuckDB
con = duckdb.connect()
# httpfs e cache + threads
con.execute(
        """
    INSTALL httpfs; LOAD httpfs;
    SET enable_object_cache = true;
    SET threads = 2;
"""
)

# ⚡️ CARICO L'INDICE IN RAM UNA SOLA VOLTA
con.execute(
        f"""
    CREATE OR REPLACE TEMP TABLE idx AS
    SELECT comune, DENOMINAZIONE_IT, file
    FROM '{INDEX_URL}';
"""
)

# Cache in memoria per l'indice comune→file_regione
index_cache = {}

@app.get("/lookup")
def lookup(comune: str, foglio: str, particella: str):
    try:
        comune = comune.strip().upper()
        foglio = foglio.strip()
        particella = particella.strip()

        # Trova file regione
        if comune not in index_cache:
            q1 = "SELECT file FROM idx WHERE comune = $1 LIMIT 1"
            res = con.execute(q1, [comune]).fetchone()
            if not res:
                raise HTTPException(status_code=404, detail="Comune non trovato nell'indice")
            index_cache[comune] = res[0]
        file_reg = index_cache[comune]

        # Query sulla particella
        q2 = f"""
            SELECT
              INSPIREID_LOCALID,
              comune,
              foglio,
              particella,
              x / 1000000.0 AS lon,
              y / 1000000.0 AS lat
            FROM '{BASE_URL}{file_reg}'
            WHERE comune = $1 AND foglio = $2 AND particella = $3
            LIMIT 1
        """
        row = con.execute(q2, [comune, foglio, particella]).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Particella non trovata")

        return {
            "localid": row[0],
            "comune": row[1],
            "foglio": row[2],
            "particella": row[3],
            "lon": row[4],
            "lat": row[5]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search_comuni")
def search_comuni(q: str, limit: int = 20):
    """
    Suggerimenti comuni per nome: ritorna [{nome, codice}] dove 'codice' è quello catastale (es. L719).
    Cerca case-insensitive su DENOMINAZIONE_IT dentro l'index.parquet.
    """
    try:
        sql = """
            SELECT DENOMINAZIONE_IT AS nome, comune AS codice
            FROM idx
            WHERE lower(DENOMINAZIONE_IT) LIKE '%' || lower($1) || '%'
            GROUP BY 1,2
            ORDER BY DENOMINAZIONE_IT
            LIMIT $2
        """
        rows = con.execute(sql, [q, limit]).fetchall()
        return [{"nome": r[0], "codice": r[1]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
def root():
    return {"status": "ok", "message": "Catasto Lookup API pronta"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

# Ensure JSON error responses; CORS middleware will append headers even on errors
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc: Exception):
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})

@app.get("/schema_regione")
def schema_regione(codice_comune: str):
    # trova file regione dall'indice
    q = "SELECT file FROM idx WHERE comune = $1 LIMIT 1"
    res = con.execute(q, [codice_comune.upper()]).fetchone()
    if not res:
        raise HTTPException(status_code=404, detail="Comune non trovato")
    file_reg = BASE_URL + res[0]
    # schema
    df = con.execute(f"DESCRIBE SELECT * FROM '{file_reg}' LIMIT 1").fetchall()
    # ritorna lista colonne
    return [{"column": r[0], "type": r[1]} for r in df]

@app.get("/check_duplicati")
def check_duplicati(codice_comune: str, limit: int = 20):
    """
    Cerca coppie (foglio, particella) duplicate per un comune nel parquet regionale.
    Ritorna le prime 'limit' occorrenze con count > 1.
    """
    # 1) parquet regionale
    q = "SELECT file FROM idx WHERE comune = $1 LIMIT 1"
    res = con.execute(q, [codice_comune.upper()]).fetchone()
    if not res:
        raise HTTPException(status_code=404, detail="Comune non trovato")
    file_reg = BASE_URL + res[0]

    # 2) cerca duplicati
    sql = f"""
        SELECT foglio, particella, COUNT(*) AS n
        FROM '{file_reg}'
        WHERE comune = $1
        GROUP BY foglio, particella
        HAVING COUNT(*) > 1
        ORDER BY n DESC, foglio, particella
        LIMIT $2
    """
    rows = con.execute(sql, [codice_comune.upper(), limit]).fetchall()
    return [{"foglio": r[0], "particella": r[1], "count": r[2]} for r in rows]

@app.get("/check_duplicati_numeric")
def check_duplicati_numeric(codice_comune: str, limit: int = 50):
    """
    Trova eventuali duplicati (stesso foglio, stessa particella) ma SOLO per particelle numeriche.
    """
    # parquet regionale
    q = "SELECT file FROM idx WHERE comune = $1 LIMIT 1"
    res = con.execute(q, [codice_comune.upper()]).fetchone()
    if not res:
        raise HTTPException(status_code=404, detail="Comune non trovato")
    file_reg = BASE_URL + res[0]

    sql = f"""
        SELECT foglio, particella, COUNT(*) AS n
        FROM '{file_reg}'
        WHERE comune = $1
          AND REGEXP_MATCHES(particella, '^[0-9]+$')  -- solo numeriche
        GROUP BY foglio, particella
        HAVING COUNT(*) > 1
        ORDER BY foglio, particella
        LIMIT $2
    """
    rows = con.execute(sql, [codice_comune.upper(), limit]).fetchall()
    return [{"foglio": r[0], "particella": r[1], "count": r[2]} for r in rows]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
