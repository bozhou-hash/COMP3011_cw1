from pydantic import BaseModel
from datetime import date
from typing import Optional


# -------- User --------
class UserCreate(BaseModel):
    username: str
    email: str
    password: str


class UserResponse(BaseModel):
    id: int
    username: str
    email: str

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str


# -------- Retailer --------
class RetailerBase(BaseModel):
    name: str


class RetailerCreate(RetailerBase):
    pass


class RetailerResponse(RetailerBase):
    id: int

    class Config:
        from_attributes = True


# -------- Product Group --------
class ProductGroupBase(BaseModel):
    group_name: str
    category: Optional[str]
    quantity: Optional[str]


class ProductGroupCreate(ProductGroupBase):
    pass


class ProductGroupResponse(ProductGroupBase):
    id: int

    class Config:
        from_attributes = True

# -------- Listing --------
class ListingBase(BaseModel):
    retailer_id: int
    original_name: str
    own_brand: bool
    category: str


class ListingCreate(ListingBase):
    pass


class Listing(ListingBase):
    id: int
    product_id: int

    class Config:
        from_attributes = True

# -------- Price --------
class PriceBase(BaseModel):
    listing_id: int
    date: date
    price: float
    unit_price: Optional[float]


class PriceCreate(PriceBase):
    pass


class PriceResponse(PriceBase):
    id: int

    class Config:
        from_attributes = True