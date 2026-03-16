from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from fastapi.security import OAuth2PasswordRequestForm

from ..database import get_db
from .. import models, schemas
from ..auth import hash_password, verify_password, create_access_token

router = APIRouter(
    prefix="/auth",
    tags=["Authentication"]
)

@router.post("/register", response_model=schemas.UserResponse)
def register(user: schemas.UserCreate, db: Session = Depends(get_db)):

    existing_user = db.query(models.User).filter(
        models.User.username == user.username
    ).first()

    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")

    hashed_pw = hash_password(user.password)

    db_user = models.User(
        username=user.username,
        email=user.email,
        hashed_password=hashed_pw
    )

    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    return db_user

@router.post("/login", response_model=schemas.Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(),
          db: Session = Depends(get_db)):

    user = db.query(models.User).filter(
        models.User.username == form_data.username
    ).first()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid username")

    if not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid password")

    token = create_access_token({"sub": user.username})

    return {
        "access_token": token,
        "token_type": "bearer"
    }