from fastapi import FastAPI
from api.database import engine
from api import models
from .routers import auth, groups, retailers, listings, prices

models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Supermarket Price Comparison API",
    description="FastAPI backend for supermarket product comparison",
    version="1.0.0"
)

app.include_router(auth.router)
app.include_router(groups.router)
app.include_router(retailers.router)
app.include_router(listings.router)
app.include_router(prices.router)

@app.get("/")
def root():
    return {"message": "Supermarket API is running"}