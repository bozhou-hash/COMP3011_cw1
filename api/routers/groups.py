from fastapi import APIRouter, Depends, HTTPException, Query, Form
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from .. import models, schemas
from ..database import get_db
from ..dependencies import verify_token

router = APIRouter(
    prefix="/groups",
    tags=["Groups"]
)


# -------------------------
# GET ALL (with filtering)
# -------------------------
@router.get(
    "/",
    response_model=List[schemas.ProductGroupResponse],
    summary="Retrieve product groups",
    description="Returns a list of product groups. Optional filters can be used to search by category or group name."
)
def get_groups(
    skip: int = Query(0),
    limit: int = Query(50),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
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
@router.get(
    "/{group_id}",
    response_model=schemas.ProductGroupResponse,
    summary="Retrieve a product group",
    description="Returns the details of a specific product group using its ID."
)
def get_group(group_id: int, db: Session = Depends(get_db)):
    group = db.query(models.ProductGroup).filter(models.ProductGroup.id == group_id).first()

    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    return group

# -------------------------
# CHEAPEST RETAILER FOR GROUP
# -------------------------
@router.get(
    "/{group_id}/cheapest",
    summary="Retrieve cheapest retailer",
    description="Returns the retailer offering the lowest price for products within a specific group."
)
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
@router.get(
    "/{group_id}/history",
    summary="Retrieve price history",
    description="Returns historical price data for all products within a specific group."
)
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
@router.post(
    "/",
    response_model=schemas.ProductGroupResponse,
    dependencies=[Depends(verify_token)],
    summary="Create product group",
    description="Creates a new product group in the database."
)
def create_group(
    group_name: str = Form(...),
    category: str = Form(...),
    db: Session = Depends(get_db)
):

    db_group = models.ProductGroup(
        group_name=group_name,
        category=category
    )

    db.add(db_group)
    db.commit()
    db.refresh(db_group)

    return db_group

# -------------------------
# UPDATE
# -------------------------
@router.put(
    "/{group_id}",
    response_model=schemas.ProductGroupResponse,
    dependencies=[Depends(verify_token)],
    summary="Update product group",
    description="Updates the details of an existing product group."
)
def update_group(
    group_id: int,
    group_name: str = Form(...),
    category: str = Form(...),
    db: Session = Depends(get_db)
):

    db_group = db.query(models.ProductGroup).filter(models.ProductGroup.id == group_id).first()

    if not db_group:
        raise HTTPException(status_code=404, detail="Group not found")

    db_group.group_name = group_name
    db_group.category = category

    db.commit()
    db.refresh(db_group)

    return db_group

# -------------------------
# DELETE
# -------------------------
@router.delete(
    "/{group_id}",
    dependencies=[Depends(verify_token)],
    summary="Delete product group",
    description="Deletes a product group from the database using its ID."
)
def delete_group(group_id: int, db: Session = Depends(get_db)):

    db_group = db.query(models.ProductGroup).filter(models.ProductGroup.id == group_id).first()

    if not db_group:
        raise HTTPException(status_code=404, detail="Group not found")

    db.delete(db_group)
    db.commit()

    return {"message": "Group deleted successfully"}