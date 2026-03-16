def test_get_groups(client):
    response = client.get("/groups/")
    assert response.status_code == 200


def test_create_group_success(client):
    payload = {
        "group_name": "Test Group",
        "category": "Test Category",
        "quantity": "1"
    }

    response = client.post("/groups/", json=payload)

    assert response.status_code in [200, 201]


def test_get_group_not_found(client):
    response = client.get("/groups/999999")

    assert response.status_code == 404


def test_update_group_not_found(client):
    payload = {
        "group_name": "Updated Name",
        "category": "Test",
        "quantity": "1"
    }

    response = client.put("/groups/999999", json=payload)

    assert response.status_code == 404


def test_delete_group_not_found(client):
    response = client.delete("/groups/999999")

    assert response.status_code == 404


def test_get_group_cheapest(client):
    response = client.get("/groups/1/cheapest")

    assert response.status_code in [200, 404]


def test_get_group_history(client):
    response = client.get("/groups/1/history")

    assert response.status_code in [200, 404]