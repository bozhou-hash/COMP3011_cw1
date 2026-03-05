from sqlalchemy import Column, Integer, String, ForeignKey, Date, Numeric, Boolean
from sqlalchemy.orm import relationship
from .database import Base


class Retailer(Base):
    __tablename__ = "retailers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)

    listings = relationship("ProductListing", back_populates="retailer")


class ProductGroup(Base):
    __tablename__ = "product_groups"

    id = Column(Integer, primary_key=True, index=True)
    group_name = Column(String, nullable=False)
    category = Column(String)
    quantity = Column(String)

    products = relationship("Product", back_populates="group")


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    product_name_clean = Column(String, nullable=False)
    product_group_id = Column(Integer, ForeignKey("product_groups.id"))

    group = relationship("ProductGroup", back_populates="products")
    listings = relationship("ProductListing", back_populates="product")


class ProductListing(Base):
    __tablename__ = "product_listings"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"))
    retailer_id = Column(Integer, ForeignKey("retailers.id"))
    original_name = Column(String)
    own_brand = Column(Boolean)
    category = Column(String)

    product = relationship("Product", back_populates="listings")
    retailer = relationship("Retailer", back_populates="listings")
    prices = relationship("Price", back_populates="listing")


class Price(Base):
    __tablename__ = "prices"

    id = Column(Integer, primary_key=True, index=True)
    listing_id = Column(Integer, ForeignKey("product_listings.id"))
    date = Column(Date, nullable=False)
    price = Column(Numeric(10, 4), nullable=False)
    unit_price = Column(Numeric(10, 4))

    listing = relationship("ProductListing", back_populates="prices")