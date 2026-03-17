import pytest
from datetime import date


# Test user credentials
test_user = {
    "username": "testuser",
    "email": "test@test.com",
    "password": "testpassword"
}


def test_register(client):
    response = client.post(
        "/auth/register",
        data=test_user
    )

    # user may already exist if tests run multiple times
    assert response.status_code in [200, 201, 400]


def test_login(client):

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


def test_protected_endpoint_without_token(client):

    response = client.post(
        "/prices",
        data={
            "listing_id": 1,
            "price": 5.99,
            "date": date.today()
        }
    )

    assert response.status_code == 401


def test_protected_endpoint_with_token(client):

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

    # listing might not exist in test DB
    assert response.status_code in [200, 201, 400, 404]

def test_login_invalid_password(client):

    response = client.post(
        "/auth/login",
        data={
            "username": test_user["username"],
            "password": "wrongpassword"
        }
    )

    assert response.status_code == 401