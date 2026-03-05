from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from .. import models, schemas
from ..database import get_db

router = APIRouter(
    prefix="/groups",
    tags=["Groups"]
)


# -------------------------
# GET ALL (with filtering)
# -------------------------
@router.get("/", response_model=List[schemas.ProductGroupResponse])
def get_groups(
    skip: int = 0,
    limit: int = 50,
    category: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.ProductGroup)

    if category:
        query = query.filter(models.ProductGroup.category.ilike(f"%{category}%"))

    if search:
        query = query.filter(models.ProductGroup.group_name.ilike(f"%{search}%"))

    return query.offset(skip).limit(limit).all()

# -------------------------
# GET ONE
# -------------------------
@router.get("/{group_id}", response_model=schemas.ProductGroupResponse)
def get_group(group_id: int, db: Session = Depends(get_db)):
    group = db.query(models.ProductGroup).filter(models.ProductGroup.id == group_id).first()

    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    return group

# -------------------------
# CHEAPEST RETAILER FOR GROUP
# -------------------------
@router.get("/{group_id}/cheapest")
def get_cheapest(group_id: int, db: Session = Depends(get_db)):

    result = (
        db.query(
            models.Retailer.name,
            func.min(models.Price.price).label("min_price")
        )
        .join(models.ProductListing, models.ProductListing.retailer_id == models.Retailer.id)
        .join(models.Price, models.Price.listing_id == models.ProductListing.id)
        .join(models.Product, models.Product.id == models.ProductListing.product_id)
        .filter(models.Product.product_group_id == group_id)
        .group_by(models.Retailer.name)
        .order_by(func.min(models.Price.price))
        .all()
    )

    if not result:
        raise HTTPException(status_code=404, detail="No prices found")

    return [
        {
            "retailer": r[0],
            "min_price": float(r[1])
        }
        for r in result
    ]

# -------------------------
# PRICE HISTORY
# -------------------------
@router.get("/{group_id}/history")
def get_price_history(group_id: int, db: Session = Depends(get_db)):

    result = (
        db.query(
            models.Price.date,
            models.Price.price,
            models.Retailer.name
        )
        .join(models.ProductListing, models.ProductListing.id == models.Price.listing_id)
        .join(models.Product, models.Product.id == models.ProductListing.product_id)
        .join(models.Retailer, models.Retailer.id == models.ProductListing.retailer_id)
        .filter(models.Product.product_group_id == group_id)
        .order_by(models.Price.date)
        .all()
    )

    if not result:
        raise HTTPException(status_code=404, detail="No history found")

    return [
        {
            "date": r[0],
            "price": float(r[1]),
            "retailer": r[2]
        }
        for r in result
    ]

# -------------------------
# CREATE
# -------------------------
@router.post("/", response_model=schemas.ProductGroupResponse)
def create_group(group: schemas.ProductGroupCreate, db: Session = Depends(get_db)):
    db_group = models.ProductGroup(**group.model_dump())
    db.add(db_group)
    db.commit()
    db.refresh(db_group)
    return db_group

# -------------------------
# UPDATE
# -------------------------
@router.put("/{group_id}", response_model=schemas.ProductGroupResponse)
def update_group(group_id: int, group: schemas.ProductGroupCreate, db: Session = Depends(get_db)):
    db_group = db.query(models.ProductGroup).filter(models.ProductGroup.id == group_id).first()

    if not db_group:
        raise HTTPException(status_code=404, detail="Group not found")

    for key, value in group.model_dump().items():
        setattr(db_group, key, value)

    db.commit()
    db.refresh(db_group)

    return db_group

# -------------------------
# DELETE
# -------------------------
@router.delete("/{group_id}")
def delete_group(group_id: int, db: Session = Depends(get_db)):
    db_group = db.query(models.ProductGroup).filter(models.ProductGroup.id == group_id).first()

    if not db_group:
        raise HTTPException(status_code=404, detail="Group not found")

    db.delete(db_group)
    db.commit()

    return {"message": "Group deleted successfully"}