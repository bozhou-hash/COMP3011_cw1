from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy.orm import Session
from typing import List
from datetime import date
from sqlalchemy import func

from .. import models, schemas
from ..database import get_db
from ..dependencies import verify_token

router = APIRouter(
    prefix="/prices",
    tags=["Prices"]
)


# -------------------------
# GET PRICE RECORDS
# -------------------------
@router.get(
    "/",
    response_model=List[schemas.PriceResponse],
    summary="Retrieve price records",
    description="Returns a paginated list of price records stored in the database."
)
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
@router.get(
    "/{price_id}",
    response_model=schemas.PriceResponse,
    summary="Retrieve a price record",
    description="Returns a specific price entry using its unique price ID."
)
def get_price(price_id: int, db: Session = Depends(get_db)):

    price = db.query(models.Price).filter(models.Price.id == price_id).first()

    if not price:
        raise HTTPException(status_code=404, detail="Price not found")

    return price


# -------------------------
# CREATE PRICE
# -------------------------
@router.post(
    "/",
    response_model=schemas.PriceResponse,
    dependencies=[Depends(verify_token)],
    summary="Create price record",
    description="Creates a new price entry for a product listing on a specific date."
)
def create_price(
    listing_id: int = Form(...),
    price: float = Form(...),
    date: date = Form(...),
    db: Session = Depends(get_db)
):

    # Check listing exists
    listing = db.query(models.ProductListing).filter(
        models.ProductListing.id == listing_id
    ).first()

    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    # Check duplicate price for same listing + date
    existing_price = db.query(models.Price).filter(
        models.Price.listing_id == listing_id,
        models.Price.date == date
    ).first()

    if existing_price:
        raise HTTPException(
            status_code=400,
            detail="Price for this listing and date already exists"
        )

    db_price = models.Price(
        listing_id=listing_id,
        price=price,
        date=date
    )

    db.add(db_price)
    db.commit()
    db.refresh(db_price)

    return db_price


# -------------------------
# UPDATE PRICE
# -------------------------
@router.put(
    "/{price_id}",
    response_model=schemas.PriceResponse,
    dependencies=[Depends(verify_token)],
    summary="Update price record",
    description="Updates an existing price entry using its ID."
)
def update_price(
    price_id: int,
    listing_id: int = Form(...),
    price: float = Form(...),
    date: date = Form(...),
    db: Session = Depends(get_db)
):

    db_price = db.query(models.Price).filter(models.Price.id == price_id).first()

    if not db_price:
        raise HTTPException(status_code=404, detail="Price not found")

    db_price.listing_id = listing_id
    db_price.price = price
    db_price.date = date

    db.commit()
    db.refresh(db_price)

    return db_price


# -------------------------
# DELETE PRICE
# -------------------------
@router.delete(
    "/{price_id}",
    dependencies=[Depends(verify_token)],
    summary="Delete price record",
    description="Deletes a price entry using its ID."
)
def delete_price(price_id: int, db: Session = Depends(get_db)):

    db_price = db.query(models.Price).filter(models.Price.id == price_id).first()

    if not db_price:
        raise HTTPException(status_code=404, detail="Price not found")

    db.delete(db_price)
    db.commit()

    return {"message": "Price deleted"}