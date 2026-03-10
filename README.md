# Supermarket Price Comparison API

## Table of Contents
1. [Introduction](#introduction)
2. [Key Features](#key-features)
3. [Tech Stack](#tech-stack)
4. [Project Structure](#project-structure)
5. [Python Requirements](#python-requirements)
6. [Setup & Installation](#setup--installation)
7. [Running the API](#running-the-api)
8. [API Functions Overview](#api-functions-overview)
9. [Common Issues & Debugging](#common-issues--debugging)
10. [Development Notes](#development-notes)

---

## Introduction

This repository contains a **RESTful backend API** built with **FastAPI** for tracking supermarket product prices.  
The API allows structured storage and retrieval of retailers, product groups, product listings, and historical price records using a **relational PostgreSQL database**.

The system is designed to support:
- Price comparison across retailers
- Historical price tracking
- Scalable data access via standard HTTP endpoints

---

## Key Features

- RESTful API design
- PostgreSQL relational database with enforced foreign keys
- CRUD operations for core entities
- Analytical endpoints (cheapest retailer, price history)
- Automatic request validation using Pydantic
- Interactive API documentation via Swagger UI

---

## Tech Stack

- **Python 3.10+**
- **FastAPI**
- **SQLAlchemy ORM**
- **PostgreSQL**
- **Pydantic**
- **Uvicorn**

---

## Project Structure

```
COMP3011_API/
│
├── api/
│   │
│   ├── main.py            # FastAPI application entry point
│   ├── database.py        # Database connection & session management
│   ├── models.py          # SQLAlchemy ORM models
│   ├── schemas.py         # Pydantic schemas
│   │
│   └── routers/
│       ├── __init__.py
│       ├── groups.py      # Product group endpoints
│       ├── retailers.py   # Retailer endpoints
│       ├── listings.py    # Product listing endpoints
│       └── prices.py      # Price endpoints
│
├── dataset_cleaner.py     # Dataset cleaning script
├── db_loader.py           # Loads cleaned dataset into PostgreSQL
└── db_test.py             # Script for testing database connection
```

---

## Python Requirements

All required Python packages are listed in `requirements.txt`.

To install dependencies: run 'pip install -r requirements.txt'

---

## Setup & Installation

### 1. Clone the Repository
Run 'git clone https://github.com/bozhou-hash/COMP3011_cw1.git'

Run 'cd COMP3011_cw1'

### 2. Create a Virtual Environment
Run 'python -m venv .venv'

Activate it:

**Windows**

Run '.venv\Scripts\activate'

**macOS / Linux**

Run 'source .venv/bin/activate'

### 3. Configure the Database
Update the PostgreSQL connection string in `database.py`: 

SQLALCHEMY_DATABASE_URL = "postgresql://username:password@localhost:5432/database_name"

Ensure that:
- PostgreSQL is running
- The database exists
- Tables are created before running the API

---

## Running the API

Start the development server:

Run 'uvicorn api.main:app --reload'

The API will be available at: http://127.0.0.1:8000

### Interactive Documentation

Swagger UI: http://127.0.0.1:8000/docs

ReDoc: http://127.0.0.1:8000/redoc

---

## API Functions Overview

### Groups
- Create, retrieve, update, delete product groups
- Retrieve cheapest retailer per group
- Retrieve historical price data per group

### Retailers
- Full CRUD operations

### Listings
- Create new product listings
- Retrieve listings
- Delete listings

### Prices
- Full CRUD operations
- Historical price tracking

---

## Common Issues & Debugging

### API Does Not Load / Keeps Loading

The port may already be in use.

Run the server on another port: 

Run 'uvicorn api.main:app --reload --port 9000'

---

### 500 Internal Server Error

Often caused by **foreign key violations** when inserting data.

Example:
- `product_id` does not exist
- `retailer_id` does not exist

Fix:
- Ensure the referenced records exist before creating listings.

---

### 422 Validation Error

Occurs when request JSON does not match the expected schema.

Fix:
- Check the request body format in **Swagger UI**
- Ensure correct data types are used.

---

### Duplicate ID Error

Occurs when manually specifying IDs.

Fix:
- Do not include `id` in POST requests
- Allow PostgreSQL to auto-generate primary keys.

---

### Stopping Uvicorn on Windows

If `CTRL + C` does not stop the server:

Run 'netstat -ano | findstr :9000'

Run 'taskkill /PID <PID> /F'

---

## Development Notes

- The API follows REST conventions for predictable behaviour.
- SQLAlchemy ORM is used to maintain relational integrity.
- Pydantic schemas enforce strict request validation.
- The project structure is modular to allow future extensions.
