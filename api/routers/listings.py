from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from .. import models
from ..database import get_db

router = APIRouter(
    prefix="/listings",
    tags=["Listings"]
)


# -------------------------
# GET ALL LISTINGS
# -------------------------
@router.get("/")
def get_listings(skip: int = 0, limit: int = 50, db: Session = Depends(get_db)):

    listings = (
        db.query(models.ProductListing)
        .offset(skip)
        .limit(limit)
        .all()
    )

    return listings


# -------------------------
# GET ONE LISTING
# -------------------------
@router.get("/{listing_id}")
def get_listing(listing_id: int, db: Session = Depends(get_db)):

    listing = db.query(models.ProductListing).filter(
        models.ProductListing.id == listing_id
    ).first()

    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    return listing


# -------------------------
# DELETE LISTING
# -------------------------
@router.delete("/{listing_id}")
def delete_listing(listing_id: int, db: Session = Depends(get_db)):

    listing = db.query(models.ProductListing).filter(
        models.ProductListing.id == listing_id
    ).first()

    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    db.delete(listing)
    db.commit()

    return {"message": "Listing deleted"}