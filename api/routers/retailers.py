from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from .. import models, schemas
from ..database import get_db

router = APIRouter(
    prefix="/retailers",
    tags=["Retailers"]
)

# -------------------------
# GET ALL
# -------------------------
@router.get("/", response_model=List[schemas.RetailerResponse])
def get_retailers(skip: int = 0, limit: int = 50, db: Session = Depends(get_db)):
    return db.query(models.Retailer).offset(skip).limit(limit).all()


# -------------------------
# GET ONE
# -------------------------
@router.get("/{retailer_id}", response_model=schemas.RetailerResponse)
def get_retailer(retailer_id: int, db: Session = Depends(get_db)):
    retailer = db.query(models.Retailer).filter(models.Retailer.id == retailer_id).first()

    if not retailer:
        raise HTTPException(status_code=404, detail="Retailer not found")

    return retailer


# -------------------------
# CREATE
# -------------------------
@router.post("/", response_model=schemas.RetailerResponse)
def create_retailer(retailer: schemas.RetailerCreate, db: Session = Depends(get_db)):

    db_retailer = models.Retailer(name=retailer.name)

    db.add(db_retailer)
    db.commit()
    db.refresh(db_retailer)

    return db_retailer


# -------------------------
# UPDATE
# -------------------------
@router.put("/{retailer_id}", response_model=schemas.RetailerResponse)
def update_retailer(retailer_id: int, retailer: schemas.RetailerCreate, db: Session = Depends(get_db)):

    db_retailer = db.query(models.Retailer).filter(models.Retailer.id == retailer_id).first()

    if not db_retailer:
        raise HTTPException(status_code=404, detail="Retailer not found")

    db_retailer.name = retailer.name

    db.commit()
    db.refresh(db_retailer)

    return db_retailer


# -------------------------
# DELETE
# -------------------------
@router.delete("/{retailer_id}")
def delete_retailer(retailer_id: int, db: Session = Depends(get_db)):

    db_retailer = db.query(models.Retailer).filter(models.Retailer.id == retailer_id).first()

    if not db_retailer:
        raise HTTPException(status_code=404, detail="Retailer not found")

    db.delete(db_retailer)
    db.commit()

    return {"message": "Retailer deleted successfully"}