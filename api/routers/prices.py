from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from sqlalchemy import func

from .. import models, schemas
from ..database import get_db
router = APIRouter(
    prefix="/prices",
    tags=["Prices"]
)


# -------------------------
# GET PRICE RECORDS
# -------------------------
@router.get("/", response_model=List[schemas.PriceResponse])
def get_prices(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):

    return (
        db.query(models.Price)
        .offset(skip)
        .limit(limit)
        .all()
    )


# -------------------------
# GET PRICE BY ID
# -------------------------
@router.get("/{price_id}", response_model=schemas.PriceResponse)
def get_price(price_id: int, db: Session = Depends(get_db)):

    price = db.query(models.Price).filter(models.Price.id == price_id).first()

    if not price:
        raise HTTPException(status_code=404, detail="Price not found")

    return price


# -------------------------
# CREATE PRICE
# -------------------------
@router.post("/", response_model=schemas.PriceResponse)
def create_price(price: schemas.PriceCreate, db: Session = Depends(get_db)):

    # Check listing exists
    listing = db.query(models.ProductListing).filter(
        models.ProductListing.id == price.listing_id
    ).first()

    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    # Check duplicate price for same listing + date
    existing_price = db.query(models.Price).filter(
        models.Price.listing_id == price.listing_id,
        models.Price.date == price.date
    ).first()

    if existing_price:
        raise HTTPException(
            status_code=400,
            detail="Price for this listing and date already exists"
        )

    db_price = models.Price(**price.model_dump())

    db.add(db_price)
    db.commit()
    db.refresh(db_price)

    return db_price


# -------------------------
# UPDATE PRICE
# -------------------------
@router.put("/{price_id}", response_model=schemas.PriceResponse)
def update_price(price_id: int, price: schemas.PriceCreate, db: Session = Depends(get_db)):

    db_price = db.query(models.Price).filter(models.Price.id == price_id).first()

    if not db_price:
        raise HTTPException(status_code=404, detail="Price not found")

    for key, value in price.model_dump().items():
        setattr(db_price, key, value)

    db.commit()
    db.refresh(db_price)

    return db_price


# -------------------------
# DELETE PRICE
# -------------------------
@router.delete("/{price_id}")
def delete_price(price_id: int, db: Session = Depends(get_db)):

    db_price = db.query(models.Price).filter(models.Price.id == price_id).first()

    if not db_price:
        raise HTTPException(status_code=404, detail="Price not found")

    db.delete(db_price)
    db.commit()

    return {"message": "Price deleted"}