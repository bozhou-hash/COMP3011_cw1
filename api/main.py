from fastapi import FastAPI
from .routers import groups, retailers, listings, prices

app = FastAPI(
    title="Supermarket Price Comparison API",
    description="FastAPI backend for supermarket product comparison",
    version="1.0.0"
)

app.include_router(groups.router)
app.include_router(retailers.router)
app.include_router(listings.router)
app.include_router(prices.router)

@app.get("/")
def root():
    return {"message": "Supermarket API is running"}

# uvicorn api.main:app --reload --host 127.0.0.1 --port 9000