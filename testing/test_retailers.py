def test_get_retailers(client):
    response = client.get("/retailers/")

    assert response.status_code == 200


def test_create_retailer_success(client):
    payload = {
        "name": "Test Retailer"
    }

    response = client.post("/retailers/", json=payload)

    assert response.status_code in [200, 201]


def test_create_retailer_invalid(client):
    payload = {}

    response = client.post("/retailers/", json=payload)

    assert response.status_code == 422


def test_get_retailer_not_found(client):
    response = client.get("/retailers/999999")

    assert response.status_code == 404