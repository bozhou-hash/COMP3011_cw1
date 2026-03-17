import pytest


test_user = {
    "username": "groupuser",
    "email": "group@test.com",
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
# GET ALL GROUPS
# -------------------------
def test_get_groups(client):

    response = client.get("/groups/")

    assert response.status_code == 200


# -------------------------
# GET GROUP NOT FOUND
# -------------------------
def test_get_group_not_found(client):

    response = client.get("/groups/999999")

    assert response.status_code == 404


# -------------------------
# FILTER GROUPS
# -------------------------
def test_get_groups_filter(client):

    response = client.get("/groups/?category=test")

    assert response.status_code == 200


def test_get_groups_search(client):

    response = client.get("/groups/?search=test")

    assert response.status_code == 200


# -------------------------
# CREATE WITHOUT TOKEN
# -------------------------
def test_create_group_no_token(client):

    response = client.post(
        "/groups/",
        data={
            "group_name": "Test Group",
            "category": "Test Category"
        }
    )

    assert response.status_code == 401


# -------------------------
# CREATE SUCCESS
# -------------------------
def test_create_group_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.post(
        "/groups/",
        headers=headers,
        data={
            "group_name": "Test Group",
            "category": "Test Category"
        }
    )

    assert response.status_code in [200, 201]


# -------------------------
# UPDATE NOT FOUND
# -------------------------
def test_update_group_not_found(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.put(
        "/groups/999999",
        headers=headers,
        data={
            "group_name": "Updated",
            "category": "Test"
        }
    )

    assert response.status_code == 404


# -------------------------
# UPDATE SUCCESS
# -------------------------
def test_update_group_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    create = client.post(
        "/groups/",
        headers=headers,
        data={
            "group_name": "Group A",
            "category": "Cat"
        }
    )

    group_id = create.json()["id"]

    response = client.put(
        f"/groups/{group_id}",
        headers=headers,
        data={
            "group_name": "Group B",
            "category": "Cat2"
        }
    )

    assert response.status_code == 200


# -------------------------
# DELETE NOT FOUND
# -------------------------
def test_delete_group_not_found(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = client.delete(
        "/groups/999999",
        headers=headers
    )

    assert response.status_code == 404


# -------------------------
# DELETE SUCCESS
# -------------------------
def test_delete_group_success(client):

    token = get_token(client)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    create = client.post(
        "/groups/",
        headers=headers,
        data={
            "group_name": "Delete Group",
            "category": "Cat"
        }
    )

    group_id = create.json()["id"]

    response = client.delete(
        f"/groups/{group_id}",
        headers=headers
    )

    assert response.status_code == 200


# -------------------------
# CHEAPEST
# -------------------------
def test_get_group_cheapest(client):

    response = client.get("/groups/1/cheapest")

    assert response.status_code in [200, 404]


# -------------------------
# HISTORY
# -------------------------
def test_get_group_history(client):

    response = client.get("/groups/1/history")

    assert response.status_code in [200, 404]