import pytest


test_user = {
    "username": "listinguser",
    "email": "listing@test.com",
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
def test_get_listings(client):

    response = client.get("/listings/")

    assert response.status_code == 200


# -------------------------
# GET ONE NOT FOUND
# -------------------------
def test_get_listing_not_found(client):

    response = client.get("/listings/999999")

    assert response.status_code == 404


# -------------------------
# CREATE WITHOUT TOKEN
# -------------------------
def test_create_listing_no_token(client):

    response = client.post(
        "/listings/",
        data={
            "retailer_id": 1,
            "original_name": "Test",
            "own_brand": "false",
            "category": "Test"
        }
    )

    assert response.status_code == 401


# -------------------------
# CREATE INVALID RETAILER
# -------------------------
def test_create_listing_invalid_retailer(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.post(
        "/listings/",
        headers=headers,
        data={
            "retailer_id": 999999,
            "original_name": "Test",
            "own_brand": "false",
            "category": "Test"
        }
    )

    assert response.status_code == 404


# -------------------------
# CREATE SUCCESS
# -------------------------
def test_create_listing_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # assumes retailer id 1 exists in test DB
    response = client.post(
        "/listings/",
        headers=headers,
        data={
            "retailer_id": 1,
            "original_name": "Milk",
            "own_brand": "false",
            "category": "Dairy"
        }
    )

    assert response.status_code in [200, 201]


# -------------------------
# DELETE NOT FOUND
# -------------------------
def test_delete_listing_not_found(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.delete(
        "/listings/999999",
        headers=headers
    )

    assert response.status_code == 404


# -------------------------
# DELETE SUCCESS
# -------------------------
def test_delete_listing_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    create = client.post(
        "/listings/",
        headers=headers,
        data={
            "retailer_id": 1,
            "original_name": "Delete Item",
            "own_brand": "false",
            "category": "Test"
        }
    )

    if create.status_code not in [200, 201]:
        pytest.skip("Retailer not in test DB")

    listing_id = create.json()["id"]

    response = client.delete(
        f"/listings/{listing_id}",
        headers=headers
    )

    assert response.status_code == 200