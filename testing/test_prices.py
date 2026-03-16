def test_get_prices(client):
    response = client.get("/prices/")

    assert response.status_code == 200


def test_create_price_success(client):
    payload = {
        "listing_id": 1,
        "date": "2024-01-01",
        "price": 1.99,
        "unit_price": 0.50
    }

    response = client.post("/prices/", json=payload)

    assert response.status_code in [200, 201]


def test_create_price_invalid_listing(client):
    payload = {
        "listing_id": 999999,
        "date": "2024-01-01",
        "price": 1.99,
        "unit_price": 0.50
    }

    response = client.post("/prices/", json=payload)

    assert response.status_code in [400, 404, 500]


def test_get_price_not_found(client):
    response = client.get("/prices/999999")

    assert response.status_code == 404


def test_delete_price_not_found(client):
    response = client.delete("/prices/999999")

    assert response.status_code == 404