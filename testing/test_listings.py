def test_get_listings(client):
    response = client.get("/listings/")

    assert response.status_code == 200


def test_create_listing_success(client):
    payload = {
        "product_id": 1,
        "retailer_id": 1,
        "original_name": "Test Product",
        "own_brand": False,
        "category": "Test"
    }

    response = client.post("/listings/", json=payload)

    assert response.status_code in [200, 201]


def test_create_listing_invalid(client):
    payload = {
        "product_id": 999999,
        "retailer_id": 999999,
        "original_name": "Invalid",
        "own_brand": False,
        "category": "Test"
    }

    response = client.post("/listings/", json=payload)

    assert response.status_code in [400, 404, 500]


def test_get_listing_not_found(client):
    response = client.get("/listings/999999")

    assert response.status_code == 404


def test_delete_listing_not_found(client):
    response = client.delete("/listings/999999")

    assert response.status_code == 404