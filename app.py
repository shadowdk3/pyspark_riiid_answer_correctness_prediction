from flask import Flask, request, render_template

from src.pipeline.predict_pipeline import CustomData, PredicPipeline

application = Flask(__name__)

app = application

## Route for a home page

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predictdata', methods=['GET', 'POST'])
def predict_datapoint():
    if request.method=='GET':
        return render_template('home.html')
    else:
        data=CustomData(
            timestamp = request.form.get('timestampAnalyst'),
            user_id = request.form.get('userIdAnalyst'),
            content_id = request.form.get('contentIdAnalyst'),
            task_container_id = request.form.get('taskContainerIdAnalyst'),
            user_answer = request.form.get('userAnswerAnalyst'),
            prior_question_elapsed_time = request.form.get('priorQuestionElapsedTimeAnalyst'),
            question_part = request.form.get('questionPartAnalyst'),
            lecture_part = request.form.get('lecturePartAnalyst'),
            bundle_id = request.form.get('bundleIdAnalyst'),
            correct_answer = request.form.get('correctAnswerAnalyst'),
        )
        pred_df = data.get_data_as_data_frame()
        print(pred_df)

        predict_pipeline = PredicPipeline()
        results = predict_pipeline.predict(pred_df)

        return render_template('home.html', results=results[0])

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)