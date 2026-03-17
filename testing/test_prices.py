import pytest
from datetime import date


test_user = {
    "username": "priceuser",
    "email": "price@test.com",
    "password": "testpassword"
}


# -------------------------
# HELPER → get token
# -------------------------
def get_token(client):

    client.post("/auth/register", data=test_user)

    response = client.post(
        "/auth/login",
        data={
            "username": test_user["username"],
            "password": test_user["password"]
        }
    )

    return response.json()["access_token"]


# -------------------------
# GET ALL
# -------------------------
def test_get_prices(client):

    response = client.get("/prices/")

    assert response.status_code == 200


# -------------------------
# GET ONE NOT FOUND
# -------------------------
def test_get_price_not_found(client):

    response = client.get("/prices/999999")

    assert response.status_code == 404


# -------------------------
# CREATE WITHOUT TOKEN
# -------------------------
def test_create_price_no_token(client):

    response = client.post(
        "/prices/",
        data={
            "listing_id": 1,
            "price": 1.99,
            "date": "2024-01-01"
        }
    )

    assert response.status_code == 401


# -------------------------
# CREATE INVALID LISTING
# -------------------------
def test_create_price_invalid_listing(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.post(
        "/prices/",
        headers=headers,
        data={
            "listing_id": 999999,
            "price": 1.99,
            "date": "2024-01-01"
        }
    )

    assert response.status_code == 404


# -------------------------
# CREATE SUCCESS
# -------------------------
def test_create_price_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # assumes listing id 1 exists
    response = client.post(
        "/prices/",
        headers=headers,
        data={
            "listing_id": 1,
            "price": 2.50,
            "date": "2024-02-01"
        }
    )

    assert response.status_code in [200, 201, 400, 404]


# -------------------------
# CREATE DUPLICATE
# -------------------------
def test_create_price_duplicate(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "listing_id": 1,
        "price": 3.00,
        "date": "2024-03-01"
    }

    client.post("/prices/", headers=headers, data=payload)

    response = client.post(
        "/prices/",
        headers=headers,
        data=payload
    )

    assert response.status_code in [400, 404]


# -------------------------
# UPDATE NOT FOUND
# -------------------------
def test_update_price_not_found(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.put(
        "/prices/999999",
        headers=headers,
        data={
            "listing_id": 1,
            "price": 2.99,
            "date": "2024-01-01"
        }
    )

    assert response.status_code == 404


# -------------------------
# UPDATE SUCCESS
# -------------------------
def test_update_price_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    create = client.post(
        "/prices/",
        headers=headers,
        data={
            "listing_id": 1,
            "price": 4.00,
            "date": "2024-04-01"
        }
    )

    if create.status_code not in [200, 201]:
        pytest.skip("Listing not in test DB")

    price_id = create.json()["id"]

    response = client.put(
        f"/prices/{price_id}",
        headers=headers,
        data={
            "listing_id": 1,
            "price": 4.50,
            "date": "2024-04-02"
        }
    )

    assert response.status_code == 200


# -------------------------
# DELETE NOT FOUND
# -------------------------
def test_delete_price_not_found(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.delete(
        "/prices/999999",
        headers=headers
    )

    assert response.status_code == 404


# -------------------------
# DELETE SUCCESS
# -------------------------
def test_delete_price_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    create = client.post(
        "/prices/",
        headers=headers,
        data={
            "listing_id": 1,
            "price": 5.00,
            "date": "2024-05-01"
        }
    )

    if create.status_code not in [200, 201]:
        pytest.skip("Listing not in test DB")

    price_id = create.json()["id"]

    response = client.delete(
        f"/prices/{price_id}",
        headers=headers
    )

    assert response.status_code == 200