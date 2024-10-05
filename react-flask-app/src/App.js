import React, { useState } from 'react';

function App() {
  const [timestamp, setTimestamp] = useState('');
  const [userId, setUserId] = useState('');
  const [contentId, setContentId] = useState('');
  const [taskContainerId, setTaskContainerId] = useState('');
  const [userAnswer, setUserAnswer] = useState('');
  const [priorQuestionElapsedTime, setPriorQuestionElapsedTime] = useState('');
  const [questionPart, setQuestionPart] = useState('');
  const [lecturePart, setLecturePart] = useState('');
  const [bundleId, setBundleId] = useState('');
  const [correctAnswer, setCorrectAnswer] = useState('');
  const [prediction, setPrediction] = useState(null);

  const timestampOptions = [
    '23615', 
    '23647', 
    '24307', 
    '24383', 
    '25481', 
    '25821', 
    '26501', 
    '26536', 
    '134577', 
    '154216'
  ];
  
  const userIdOptions = [
    '338684437',
    '455973631',
    '801103753',
    '1047202059',
    '1478712595',
    '1547278749',
    '1660941992',
    '1743444187',
    '1842816145',
    '2146130037'
  ];

  const contentIdOptions = [
    '175',
    '2063',
    '2064',
    '2065',
    '4120',
    '4696',
    '6116',
    '6173',
    '7876',
    '7900'
  ];

  const taskContainerIdOptions = [
    '0',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
    '10',
    '14',
    '15'
  ];

  const userAnswerOptions = [
    '0',
    '1',
    '2',
    '3'
  ];

  const priorQuestionElapsedTimeOptions = [
    '14000.0',    
    '15000.0',
    '16000.0',
    '17000.0',
    '18000.0',
    '19000.0',
    '20000.0',
    '21000.0',
    '22000.0',
    '23000.0'
  ];
  
  const questionPartOptions = [
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7'
  ];

  const lecturePartOptions = [
    '-1',
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7'
  ];

  const bundleIdOptions = [
    '2063',
    '2593',
    '2946',
    '3363',
    '4120',
    '6116',
    '6173',
    '6877',
    '6908',
    '7216'
  ];

  const correctAnswerOptions = [
    '0',
    '1',
    '2',
    '3'
  ];

  const handleSubmit = async (e) => {
    e.preventDefault();

    const formData = {
      timestamp: timestamp,
      userId: userId,
      contentId: contentId,
      taskContainerId: taskContainerId,
      userAnswer: userAnswer,
      priorQuestionElapsedTime: priorQuestionElapsedTime,
      questionPart: questionPart,
      bundleId: bundleId,
      lecturePart: lecturePart,
      correctAnswer: correctAnswer,
    };

    try {
      const response = await fetch('/predict_datapoint', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(formData),
      });

      const result = await response.json();
      setPrediction(result.result);
    } catch (error) {
      console.error('Error:', error);
    }
  };

  return (
    <div className="App">
      <h1>Riiid Answer Correctness Prediction</h1>
      <form onSubmit={handleSubmit}>
        
        <div>
          <label>Timestamp: </label>
          <select
            value={timestamp}
            onChange={(e) => setTimestamp(e.target.value)}
          >
            <option value="">Select Timestamp</option>
            {timestampOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>
        
        <div>
          <label>User ID: </label>
          <select
            value={userId}
            onChange={(e) => setUserId(e.target.value)}
          >
            <option value="">Select User ID</option>
            {userIdOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Content ID: </label>
          <select
            value={contentId}
            onChange={(e) => setContentId(e.target.value)}
          >
            <option value="">Select Content ID</option>
            {contentIdOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Task Container ID: </label>
          <select
            value={taskContainerId}
            onChange={(e) => setTaskContainerId(e.target.value)}
          >
            <option value="">Select Task Content ID</option>
            {taskContainerIdOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>User Answer: </label>
          <select
            value={userAnswer}
            onChange={(e) => setUserAnswer(e.target.value)}
          >
            <option value="">Select User Answer</option>
            {userAnswerOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Prior Question Elapsed Time: </label>
          <select
            value={priorQuestionElapsedTime}
            onChange={(e) => setPriorQuestionElapsedTime(e.target.value)}
          >
            <option value="">Select Prior Question Elapsed Time</option>
            {priorQuestionElapsedTimeOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Question Part: </label>
          <select
            value={questionPart}
            onChange={(e) => setQuestionPart(e.target.value)}
          >
            <option value="">Select Question Part</option>
            {questionPartOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Lecture Part: </label>
          <select
            value={lecturePart}
            onChange={(e) => setLecturePart(e.target.value)}
          >
            <option value="">Select Lecture Part</option>
            {lecturePartOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Bundle ID: </label>
          <select
            value={bundleId}
            onChange={(e) => setBundleId(e.target.value)}
          >
            <option value="">Select Bundle ID</option>
            {bundleIdOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Correct Answer: </label>
          <select
            value={correctAnswer}
            onChange={(e) => setCorrectAnswer(e.target.value)}
          >
            <option value="">Select Correct Answer</option>
            {correctAnswerOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>

        <button type="submit">Predict</button>
      </form>

      {prediction && <h2>Prediction: {prediction}</h2>}
    </div>
  );
}

export default App;