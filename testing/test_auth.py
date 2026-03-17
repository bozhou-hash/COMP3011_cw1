import pytest
from datetime import date


test_user = {
    "username": "testuser",
    "email": "test@test.com",
    "password": "testpassword"
}


# -------------------------
# REGISTER SUCCESS / DUPLICATE
# -------------------------
def test_register(client):

    response = client.post(
        "/auth/register",
        data=test_user
    )

    assert response.status_code in [200, 400]


def test_register_duplicate_username(client):

    client.post("/auth/register", data=test_user)

    response = client.post(
        "/auth/register",
        data=test_user
    )

    assert response.status_code == 400


# -------------------------
# REGISTER MISSING FIELD
# -------------------------
def test_register_missing_field(client):

    response = client.post(
        "/auth/register",
        data={
            "username": "user2",
            "email": "email@test.com"
            # missing password
        }
    )

    assert response.status_code == 422


# -------------------------
# LOGIN SUCCESS
# -------------------------
def test_login(client):

    client.post("/auth/register", data=test_user)

    response = client.post(
        "/auth/login",
        data={
            "username": test_user["username"],
            "password": test_user["password"]
        }
    )

    assert response.status_code == 200

    body = response.json()

    assert "access_token" in body
    assert body["token_type"] == "bearer"


# -------------------------
# LOGIN WRONG PASSWORD
# -------------------------
def test_login_invalid_password(client):

    client.post("/auth/register", data=test_user)

    response = client.post(
        "/auth/login",
        data={
            "username": test_user["username"],
            "password": "wrongpassword"
        }
    )

    assert response.status_code == 401


# -------------------------
# LOGIN WRONG USERNAME
# -------------------------
def test_login_invalid_username(client):

    response = client.post(
        "/auth/login",
        data={
            "username": "doesnotexist",
            "password": "test"
        }
    )

    assert response.status_code == 401


# -------------------------
# LOGIN MISSING FIELD
# -------------------------
def test_login_missing_field(client):

    response = client.post(
        "/auth/login",
        data={
            "username": test_user["username"]
        }
    )

    assert response.status_code == 422


# -------------------------
# TOKEN FORMAT CHECK
# -------------------------
def test_token_format(client):

    client.post("/auth/register", data=test_user)

    response = client.post(
        "/auth/login",
        data={
            "username": test_user["username"],
            "password": test_user["password"]
        }
    )

    token = response.json()["access_token"]

    assert isinstance(token, str)
    assert len(token) > 10


# -------------------------
# PROTECTED WITHOUT TOKEN
# -------------------------
def test_protected_without_token(client):

    response = client.post(
        "/prices",
        data={
            "listing_id": 1,
            "price": 5.99,
            "date": date.today()
        }
    )

    assert response.status_code == 401


# -------------------------
# PROTECTED WITH INVALID TOKEN
# -------------------------
def test_protected_invalid_token(client):

    headers = {
        "Authorization": "Bearer invalidtoken"
    }

    response = client.post(
        "/prices",
        headers=headers,
        data={
            "listing_id": 1,
            "price": 5.99,
            "date": date.today()
        }
    )

    assert response.status_code in [401, 403]


# -------------------------
# PROTECTED WITH VALID TOKEN
# -------------------------
def test_protected_with_token(client):

    client.post("/auth/register", data=test_user)

    login_response = client.post(
        "/auth/login",
        data={
            "username": test_user["username"],
            "password": test_user["password"]
        }
    )

    token = login_response.json()["access_token"]

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.post(
        "/prices",
        headers=headers,
        data={
            "listing_id": 1,
            "price": 5.99,
            "date": date.today()
        }
    )

    assert response.status_code in [200, 201, 400, 404]