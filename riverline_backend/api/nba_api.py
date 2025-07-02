from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from nba.nba_engine import NBAEngine
from pipeline.connectors.clickhouse_connector import ClickHouseConnector
from pipeline.connectors.supabase_connector import SupabaseConnector
import os

class NBARequest(BaseModel):
    customer_id: str

def create_app(db_target: str):
    """Create FastAPI app with specific database config"""
    app = FastAPI(title="Riverline NBA API")
    

    def get_db_connector():
        if db_target == "supabase":
            return SupabaseConnector(url=os.getenv("SUPABASE_URL"), key=os.getenv("SUPABASE_KEY"))
        else:  # clickhouse
            return ClickHouseConnector(
                host=os.getenv("CLICKHOUSE_HOST"),
                port=int(os.getenv("CLICKHOUSE_PORT")),
                user=os.getenv("CLICKHOUSE_USER"), 
                password=os.getenv("CLICKHOUSE_PASSWORD")
            )

    @app.post("/predict_nba")
    async def predict_nba(request: NBARequest):
        customer_id = request.customer_id
            
        try:
            connector = get_db_connector()
            print('connector used is ',connector)
            connector.connect()
            
            nba_engine = NBAEngine(connector)
            prediction = nba_engine.predict_for_customer(customer_id)
            
            return prediction
            
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"NBA prediction failed: {str(e)}")

    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "service": "NBA Engine", "db_target": db_target}
    
    return app
