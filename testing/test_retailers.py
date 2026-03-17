import pytest
import time


test_user = {
    "username": "retaileruser",
    "email": "retailer@test.com",
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
def test_get_retailers(client):

    response = client.get("/retailers/")

    assert response.status_code == 200


# -------------------------
# GET ONE NOT FOUND
# -------------------------
def test_get_retailer_not_found(client):

    response = client.get("/retailers/999999")

    assert response.status_code == 404


# -------------------------
# CREATE WITHOUT TOKEN
# -------------------------
def test_create_retailer_no_token(client):

    response = client.post(
        "/retailers/",
        data={
            "name": "No Token Retailer"
        }
    )

    assert response.status_code == 401


# -------------------------
# CREATE INVALID (missing name)
# -------------------------
def test_create_retailer_invalid(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.post(
        "/retailers/",
        headers=headers,
        data={}
    )

    assert response.status_code == 422


# -------------------------
# CREATE SUCCESS
# -------------------------
def test_create_retailer_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    name = f"Test Retailer {time.time()}"

    response = client.post(
        "/retailers/",
        headers=headers,
        data={
            "name": name
        }
    )

    assert response.status_code in [200, 201]


# -------------------------
# UPDATE NOT FOUND
# -------------------------
def test_update_retailer_not_found(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.put(
        "/retailers/999999",
        headers=headers,
        data={
            "name": "Updated"
        }
    )

    assert response.status_code == 404


# -------------------------
# UPDATE SUCCESS
# -------------------------
def test_update_retailer_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    name1 = f"RetailerA_{time.time()}"
    name2 = f"RetailerB_{time.time()}"

    create = client.post(
        "/retailers/",
        headers=headers,
        data={
            "name": name1
        }
    )

    retailer_id = create.json()["id"]

    response = client.put(
        f"/retailers/{retailer_id}",
        headers=headers,
        data={
            "name": name2
        }
    )

    assert response.status_code == 200


# -------------------------
# DELETE NOT FOUND
# -------------------------
def test_delete_retailer_not_found(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.delete(
        "/retailers/999999",
        headers=headers
    )

    assert response.status_code == 404


# -------------------------
# DELETE SUCCESS
# -------------------------
def test_delete_retailer_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    create = client.post(
        "/retailers/",
        headers=headers,
        data={
            "name": "Delete Retailer"
        }
    )

    retailer_id = create.json()["id"]

    response = client.delete(
        f"/retailers/{retailer_id}",
        headers=headers
    )

    assert response.status_code == 200