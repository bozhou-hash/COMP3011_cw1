# Supermarket Price Comparison API

## Table of Contents
1. [Introduction](#introduction)
2. [Live Deployment](#live-deployment)
3. [Key Features](#key-features)
4. [Tech Stack](#tech-stack)
5. [Project Structure](#project-structure)
6. [Python Requirements](#python-requirements)
7. [Setup & Installation](#setup--installation)
8. [Running the API](#running-the-api)
9. [API Functions Overview](#api-functions-overview)
10. [Authentication](#authentication)
11. [Testing](#testing)
12. [API Endpoint Testing](#api-endpoint-testing)
13. [Common Issues & Debugging](#common-issues--debugging)
14. [Development Notes](#development-notes)

---

## Introduction

This repository contains a **RESTful backend API** built with **FastAPI** for tracking supermarket product prices.  
The API allows structured storage and retrieval of retailers, product groups, product listings, and historical price records using a **relational PostgreSQL database**.

The system is designed to support:
- Price comparison across retailers
  - Historical price tracking
  - Scalable data access via standard HTTP endpoints

---

## Live Deployment

The API is deployed in production using **cloud infrastructure**.

### Production API
https://web-production-f914.up.railway.app

### Interactive API Documentation
https://web-production-f914.up.railway.app/docs

The interactive documentation allows users to:
- Explore available endpoints
  - Test API requests directly in the browser
  - Inspect request and response schema

---

### Database Note
The production deployment uses a **trimmed dataset (~25,000 products)**.

The original dataset contains several million price records, but it has been reduced due to storage limitations of the free PostgreSQL tier provided by Railway.

The reduced dataset still preserves:
- Realistic relational structure
  - Representative price history
  - Full API functionality

This allows the API to be fully demonstrated while remaining within free-tier infrastructure limits.

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

Deployment infrastructure:
- **Railway (API hosting + PostgreSQL database)**
  - **Github (source control)**

---

## Project Structure

```
COMP3011_API/
│
├── api/
│   ├── auth.py            # Define functions for authentication token
│   ├── main.py            # FastAPI application entry point
│   ├── database.py        # Database connection & session management
│   ├── models.py          # SQLAlchemy ORM models
│   ├── schemas.py         # Pydantic schemas
│   ├── dependencies.py    # Define functions to verify authentication token
│   └── routers/
│       ├── __init__.py
│       ├── auth.py        # Authentication endpoints
│       ├── groups.py      # Product group endpoints
│       ├── retailers.py   # Retailer endpoints
│       ├── listings.py    # Product listing endpoints
│       └── prices.py      # Price endpoints
│
├── testing/
│   ├── conftest.py
│   ├── test_auth.py       # Tests for authentication endpoints
│   ├── test_groups.py     # Tests for product group endpoints
│   ├── test_listings.py   # Tests for listing endpoints
│   ├── test_prices.py     # Tests for price endpoints
│   ├── test_retailers.py  # Test for retailer endpoints
│   └── test_root.py       # Test for root endpoint
│
├── dataset_cleaner.py     # Dataset cleaning script
├── db_loader.py           # Loads cleaned dataset into PostgreSQL
├── db_test.py             # Script for testing database connection
├── product_grouping.py    # Group products based on similarity in names across retailers
└── trim_db_full.py        # Trim database to smaller size to fit for web deployment
```

---

## Python Requirements

All required Python packages are listed in `requirements.txt`.

To install dependencies:

```
pip install -r requirements.txt
```

---

## Setup & Installation

### 1. Clone the Repository
Run 

```
git clone https://github.com/bozhou-hash/COMP3011_cw1.git
cd COMP3011_cw1
```

### 2. Create a Virtual Environment
Run 
```
python -m venv .venv
```

Activate it:

**Windows**

```
.venv\Scripts\activate
```

**macOS / Linux**

```
source .venv/bin/activate
```

### 3. Configure the Database
Update the PostgreSQL connection string in `database.py`: 

```
DATABASE_URL = postgresql://username:password@localhost:5432/database_name
```

Ensure that:
- PostgreSQL is running
  - The database exists
  - Tables are created before running the API

---

## Running the API

Start the development server:

```
uvicorn api.main:app --reload
```

The API will be available at: 
```
http://127.0.0.1:8000
```

### Interactive Documentation

Local Swagger UI: 
```
http://127.0.0.1:8000/docs
```

ReDoc: 
```
http://127.0.0.1:8000/redoc
```

---

## API Functions Overview

### Authentication
- Register new users
  - User logins

### Groups
- Create product groups
  - Retrieve product groups 
  - Update product groups 
  - Delete product groups 
  - Retrieve cheapest retailer per group
  - Retrieve historical price data per group

### Retailers
- Create retailers 
  - Retrieve retailers 
  - Update retailers 
  - Delete retailers

### Listings
- Create new product listings
  - Retrieve product listings
  - Delete product listings

### Prices
- Create price records 
  - Retrieve price history 
  - Update price records 
  - Delete price records

---

## Authentication

The API uses **JWT (JSON Web Tokens)** to secure protected endpoints.  
Users must register and log in to obtain a token, which is required for creating, updating, or deleting resources.

---

### Register a User

**Endpoint:** POST/auth/register

**Form Data:**

| Field     | Type   | Description          |
|-----------|--------|--------------------|
| username  | string | Unique username     |
| email     | string | User email address  |
| password  | string | Plain-text password |

**Example Request:**

```
curl -X POST "http://127.0.0.1:8000/auth/register" \
  -F "username=testuser" \
  -F "email=test@test.com" \
  -F "password=testpassword"
```

**Response:**

```JSON
{
  "id": 1,
  "username": "testuser",
  "email": "test@test.com"
}
```
---

### Login

**Endpoint:** POST/auth/login

**Form Data:**

| Field     | Type   | Description     |
|-----------|--------|-----------------|
| username  | string | User's username |
| password  | string | User's password |

**Example Request:**

```
curl -X POST "http://127.0.0.1:8000/auth/login" \
  -F "username=testuser" \
  -F "password=testpassword"
```

**Response:**

```JSON
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```
---

### Using the Token

Include the JWT token in the ```Authorization``` header when calling protected endpoints:
```
Authorization: Bearer <access_token>
```

**Example with cURL:**
```
curl -X POST "http://127.0.0.1:8000/groups/" \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -F "group_name=Milk" \
  -F "category=Dairy"
```
---

### Protected Endpoints

The following endpoints require a valid JWT token:
- ```POST/groups/``` - Create a product group
- ```PUT/groups/{group_id}``` - Update a product group
- ```DELETE/groups/{group_id}``` - Delete a product group
- ```POST/retailers/``` - Create a retailer
- ```PUT/retailers/{retailer_id}``` - Update a retailer
- ```DELETE/retailers/{retailer_id}``` - Delete a retailer
- ```POST/listings/``` - Create a product listing
- ```DELETE/listings/{listing_id}``` - Delete a product listing
- ```POST/prices/``` - Create a price record
- ```PUT/prices/{price_id}``` - Update a price record
- ```DELETE/prices/{price_id}``` - Delete a price record

If no valid token is provided, the API will return:
```JSON
{
  "detail": "Not authenticated"
}
```

or HTTP status code:
```
401 Unauthorized
```
---

## Testing

The project includes automated tests written using pytest and FastAPI TestClient.
Each router has a dedicated test file to verify correct API behaviour, status codes, and validation rules.

Tests are located in:

```
testing/
```

### Test Files Overview

| Test File | Purpose |
|-----------|---------|
| `test_root.py` | Tests the root endpoint to confirm the API is running |
| `test_auth.py` | Tests user registration and login, including token generation |
| `test_groups.py` | Tests CRUD operations for product groups and analytical endpoints |
| `test_retailers.py` | Tests CRUD operations for retailers |
| `test_listings.py` | Tests listing creation and validation of foreign key relations |
| `test_prices.py` | Tests price history creation, update, delete, and validation rules |
| `conftest.py` | Provides shared pytest fixtures such as TestClient and authentication helper |

---

### Running Tests

All tests use pytest. Make sure your environment is activated before running.

### Run All Tests
```
pytest testing
```
or simply:
```
pytest
```

### Run a Single Test File
```
pytest testing/test_retailers.py
pytest testing/test_prices.py
pytest testing/test_groups.py
pytest testing/test_listings.py
pytest testing/test_auth.py
pytest testing/test_root.py
```

### Run One Specific Test
```
pytest testing/test_retailers.py::test_create_retailer_success
pytest testing/test_prices.py::test_create_price_success
```

### Running Tests in PyCharm
1. Right-click the test file in the Project Explorer
   2. Select Run pytest
   3. View test results in the Run window

---

### Test Requirements

Before running tests, ensure:
- PosgreSQL is running
  - ```DATABASE_URL``` in ```database.py``` is correct
  - Tables exist in the database
  - At least one user can be created for authentication tests

Some tests require valid foreign keys, so the database must allow inserts.

---

### Notes About Test Data

Tests create temporary records in the database.

To avoid duplicate key errors, tests use unique values when inserting data.

If duplicates occur, you can clear the relevant tables:

```SQL
DELETE FROM retailers;
DELETE FROM prices;
DELETE FROM listings;
DELETE FROM product_groups;
```
Or reset sequences:
```SQL
SELECT setval(
    pg_get_serial_sequence('retailers','id'),
    (SELECT MAX(id) FROM retailers)
);
```

---

### Test Coverage

The test suite ensures correct behaviour for:
- Authentication
  - Product groups endpoints
  - Retailers endpoints
  - Listings endpoints
  - Prices endpoints
  - Root endpoint

This provides full coverage of all implemented API functionality.

---

## API Endpoint Testing

A complete demonstration of all implemented API endpoints is provided in the following document.

This report contains:
- Executed requests for every endpoint
  - Returned JSON responses
  - HTTP status codes (200, 201, 404, etc.)
  - Validation of CRUD functionality

API Endpoint Testing Report

[https://github.com/bozhou-hash/COMP3011_cw1/blob/main/API%20Documentation.pdf](https://github.com/bozhou-hash/COMP3011_cw1/blob/main/API%20Documentation.pdf)

---

## Common Issues & Debugging

### API Does Not Load / Keeps Loading

The port may already be in use.

Run the server on another port: 

```
uvicorn api.main:app --reload --port 9000
```

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

Occurs when database sequences become misaligned.

Fix by resetting the PostgreSQL sequence:
```SQL
SELECT setval(
    pg_get_serial_sequence('product_groups','id'),
    (SELECT MAX(id) FROM product_groups)
);
```

---

### Stopping Uvicorn on Windows

If `CTRL + C` does not stop the server:

```
netstat -ano | findstr :9000
taskkill /PID <PID> /F
```

---

## Development Notes

- The API follows REST conventions for predictable behaviour.
  - SQLAlchemy ORM is used to maintain relational integrity.
  - Pydantic schemas enforce strict request validation.
  - The project structure is modular to allow future extensions.
