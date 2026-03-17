import requests

BASE_URL = "http://127.0.0.1:9000"

# Test user credentials
test_user = {
    "username": "testuser",
    "email": "test@test.com",
    "password": "testpassword"
}


def test_register():
    print("\nRunning REGISTER test...")

    response = requests.post(
        f"{BASE_URL}/auth/register",
        json=test_user
    )

    if response.status_code in [200, 201]:
        print("PASS: User registered successfully")
    elif response.status_code == 400:
        print("INFO: User already exists (acceptable)")
    else:
        print("FAIL:", response.status_code, response.text)


def test_login():
    print("\nRunning LOGIN test...")

    response = requests.post(
        f"{BASE_URL}/auth/login",
        data={
            "username": test_user["username"],
            "password": test_user["password"]
        }
    )

    if response.status_code == 200:
        token = response.json().get("access_token")
        print("PASS: Login successful")
        return token
    else:
        print("FAIL:", response.status_code, response.text)
        return None


def test_protected_endpoint_without_token():
    print("\nRunning UNAUTHORISED ACCESS test...")

    response = requests.post(
        f"{BASE_URL}/prices",
        json={
            "listing_id": 1,
            "price": 5.99,
            "date": "2024-01-01"
        }
    )

    if response.status_code == 401:
        print("PASS: Endpoint correctly blocked unauthorised request")
    else:
        print("FAIL:", response.status_code)


def test_protected_endpoint_with_token():

    print("\nRunning AUTHORISED ACCESS test...")

    token = test_login()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.post(
        f"{BASE_URL}/prices",
        headers=headers,
        json={
            "listing_id": 1,
            "price": 5.99,
            "date": "2024-01-01"
        }
    )

    if response.status_code in [200, 201]:
        print("PASS: Authenticated request succeeded")
    elif response.status_code == 404:
        print("INFO: Listing does not exist (endpoint protected correctly)")
    else:
        print("FAIL:", response.status_code, response.text)


if __name__ == "__main__":

    print("\n==============================")
    print("AUTHENTICATION TESTS STARTING")
    print("==============================")

    test_register()

    token = test_login()

    test_protected_endpoint_without_token()

    if token:
        test_protected_endpoint_with_token(token)

    print("\n==============================")
    print("AUTHENTICATION TESTS COMPLETE")
    print("==============================")