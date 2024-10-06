import pytest
from flask import json
from app_react import app  # Adjust this import according to your application structure

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_predict_datapoint(client):
    # Sample data to be sent to the endpoint
    sample_data = {
        "timestamp": "23615",
        "userId": "338684437",
        "contentId": "2063",
        "taskContainerId": "4",
        "userAnswer": "1",
        "priorQuestionElapsedTime": "15000.0",
        "questionPart": "3",
        "bundleId": "2063",
        "lecturePart": "2",
        "correctAnswer": "0"
    }

    response = client.post('/predict_datapoint', 
                            data=json.dumps(sample_data), 
                            content_type='application/json')
    
    print("Status Code:", response.status_code)  # Debugging line
    print("Response Data:", response.data)  # Debugging line
    
    assert response.status_code == 200  # Check if the response status code is 200
    response_json = response.get_json()
    
    # Assuming the result is returned in a field named 'result'
    assert 'result' in response_json
    # Extract the numerical part from the string
    prediction_str = response_json['result']
    assert isinstance(prediction_str, str)  # Check it's a string
    
    # Extracting the numerical value
    numerical_prediction = float(prediction_str.split(": ")[-1])
    assert isinstance(numerical_prediction, (int, float))