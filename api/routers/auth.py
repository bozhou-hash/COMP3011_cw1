from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy.orm import Session

from ..database import get_db
from .. import models, schemas
from ..auth import hash_password, verify_password, create_access_token

router = APIRouter(
    prefix="/auth",
    tags=["Authentication"]
)

# -------------------------
# REGISTER USER
# -------------------------
@router.post(
    "/register",
    response_model=schemas.UserResponse,
    summary="Register a new user",
    description="Creates a new user account. The password will be securely hashed before being stored in the database."
)
def register(
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):

    existing_user = db.query(models.User).filter(
        models.User.username == username
    ).first()

    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")

    hashed_pw = hash_password(password)

    db_user = models.User(
        username=username,
        email=email,
        hashed_password=hashed_pw
    )

    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    return db_user


# -------------------------
# USER LOGIN
# -------------------------
@router.post(
    "/login",
    response_model=schemas.Token,
    summary="User login",
    description="Authenticates a user using their username and password and returns a JWT access token."
)
def login(
    username: str = Form(..., description="User's username"),
    password: str = Form(..., description="User's password"),
    db: Session = Depends(get_db)
):

    user = db.query(models.User).filter(
        models.User.username == username
    ).first()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid username")

    if not verify_password(password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid password")

    token = create_access_token({"sub": user.username})

    return {
        "access_token": token,
        "token_type": "bearer"
    }