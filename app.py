import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

INDEX_URL = "https://raw.githubusercontent.com/ondata/dati_catastali/main/S_0000_ITALIA/anagrafica/index.parquet"
BASE_URL  = "https://raw.githubusercontent.com/ondata/dati_catastali/main/S_0000_ITALIA/anagrafica/"

app = FastAPI(title="Catasto Lookup API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Connessione DuckDB
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs; SET enable_object_cache=true;")

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
            q1 = f"SELECT file FROM '{INDEX_URL}' WHERE comune = $1 LIMIT 1"
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


@app.get("/")
def root():
    return {"status": "ok", "message": "Catasto Lookup API pronta"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
