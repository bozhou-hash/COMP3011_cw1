from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List

from .. import models, schemas
from ..database import get_db
from ..dependencies import verify_token

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
# CREATE LISTING
# -------------------------
@router.post("/", response_model=schemas.Listing, dependencies=[Depends(verify_token)])
def create_listing(listing: schemas.ListingCreate, db: Session = Depends(get_db)):

    retailer = db.query(models.Retailer).filter(
        models.Retailer.id == listing.retailer_id
    ).first()

    if not retailer:
        raise HTTPException(status_code=404, detail="Retailer not found")

    product = db.query(models.Product).filter(
        func.lower(models.Product.product_name_clean) == listing.original_name.lower()
    ).first()

    if not product:
        product = models.Product(
            product_name_clean=listing.original_name
        )
        db.add(product)
        db.commit()
        db.refresh(product)

    new_listing = models.ProductListing(
        product_id=product.id,
        retailer_id=listing.retailer_id,
        original_name=listing.original_name,
        own_brand=listing.own_brand,
        category=listing.category
    )

    db.add(new_listing)
    db.commit()
    db.refresh(new_listing)

    return new_listing


# -------------------------
# DELETE LISTING
# -------------------------
@router.delete("/{listing_id}", dependencies=[Depends(verify_token)])
def delete_listing(listing_id: int, db: Session = Depends(get_db)):

    listing = db.query(models.ProductListing).filter(
        models.ProductListing.id == listing_id
    ).first()

    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    db.delete(listing)
    db.commit()

    return {"message": "Listing deleted"}