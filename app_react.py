from flask import Flask, send_from_directory, jsonify, request
import os
from src.pipeline.predict_pipeline import CustomData, PredicPipeline

app = Flask(__name__, static_folder='react-flask-app/build', static_url_path='')

# Serve React static files
@app.route('/')
def serve_react_app():
    return send_from_directory(app.static_folder, 'index.html')

# Serve other static files (like JS, CSS)
@app.route('/<path:path>')
def serve_static_files(path):
    return send_from_directory(app.static_folder, path)

# API endpoint for predictions
@app.route('/predict_datapoint', methods=['POST'])
def predict_datapoint():
    data = request.json
    custom_data=CustomData(
        timestamp = data.get('timestamp'),
        user_id = data.get('userId'),
        content_id = data.get('contentId'),
        task_container_id = data.get('taskContainerId'),
        user_answer = data.get('userAnswer'),
        prior_question_elapsed_time = data.get('priorQuestionElapsedTime'),
        question_part = data.get('questionPart'),
        lecture_part = data.get('lecturePart'),
        bundle_id = data.get('bundleId'),
        correct_answer = data.get('correctAnswer'),
    )

    pred_df = custom_data.get_data_as_data_frame()

    predict_pipeline = PredicPipeline()
    results = predict_pipeline.predict(pred_df)
    
    prediction_result = f"Predicted Answer Correctness for user {custom_data.user_id}: {results[0]}"
    
    return jsonify(result=prediction_result)

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)