from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy.orm import Session
from typing import List

from .. import models, schemas
from ..database import get_db
from ..dependencies import verify_token

router = APIRouter(
    prefix="/retailers",
    tags=["Retailers"]
)

# -------------------------
# GET ALL
# -------------------------
@router.get(
    "/",
    response_model=List[schemas.RetailerResponse],
    summary="Retrieve retailers",
    description="Returns a list of retailers stored in the database."
)
def get_retailers(skip: int = 0, limit: int = 50, db: Session = Depends(get_db)):
    return db.query(models.Retailer).offset(skip).limit(limit).all()


# -------------------------
# GET ONE
# -------------------------
@router.get(
    "/{retailer_id}",
    response_model=schemas.RetailerResponse,
    summary="Retrieve retailer",
    description="Returns the details of a specific retailer using its ID."
)
def get_retailer(retailer_id: int, db: Session = Depends(get_db)):

    retailer = db.query(models.Retailer).filter(models.Retailer.id == retailer_id).first()

    if not retailer:
        raise HTTPException(status_code=404, detail="Retailer not found")

    return retailer


# -------------------------
# CREATE
# -------------------------
@router.post(
    "/",
    response_model=schemas.RetailerResponse,
    dependencies=[Depends(verify_token)],
    summary="Create retailer",
    description="Creates a new retailer in the database."
)
def create_retailer(
    name: str = Form(..., description="Name of the retailer"),
    db: Session = Depends(get_db)
):

    db_retailer = models.Retailer(name=name)

    db.add(db_retailer)
    db.commit()
    db.refresh(db_retailer)

    return db_retailer


# -------------------------
# UPDATE
# -------------------------
@router.put(
    "/{retailer_id}",
    response_model=schemas.RetailerResponse,
    dependencies=[Depends(verify_token)],
    summary="Update retailer",
    description="Updates the name of an existing retailer."
)
def update_retailer(
    retailer_id: int,
    name: str = Form(..., description="Updated retailer name"),
    db: Session = Depends(get_db)
):

    db_retailer = db.query(models.Retailer).filter(models.Retailer.id == retailer_id).first()

    if not db_retailer:
        raise HTTPException(status_code=404, detail="Retailer not found")

    db_retailer.name = name

    db.commit()
    db.refresh(db_retailer)

    return db_retailer


# -------------------------
# DELETE
# -------------------------
@router.delete(
    "/{retailer_id}",
    dependencies=[Depends(verify_token)],
    summary="Delete retailer",
    description="Deletes a retailer from the database using its ID."
)
def delete_retailer(retailer_id: int, db: Session = Depends(get_db)):

    db_retailer = db.query(models.Retailer).filter(models.Retailer.id == retailer_id).first()

    if not db_retailer:
        raise HTTPException(status_code=404, detail="Retailer not found")

    db.delete(db_retailer)
    db.commit()

    return {"message": "Retailer deleted successfully"}