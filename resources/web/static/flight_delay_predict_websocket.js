var socket = io();
var currentUUID = null;
var delayLabels = {
  0: 'Early or on time (< -15 min)',
  1: 'On time (-15 to 0 min)',
  2: 'Minor delay (0 to 30 min)',
  3: 'Severe delay (> 30 min)'
};
document.getElementById('flight_delay_classification').addEventListener('submit', function(e) {
  e.preventDefault();
  document.getElementById('result').innerHTML = 'Processing...';
  currentUUID = null;
  var formData = new FormData(this);
  var params = new URLSearchParams(formData);
  fetch('/flights/delays/predict/classify_realtime', {
    method: 'POST',
    body: params
  })
  .then(function(r){return r.json();})
  .then(function(data){
    currentUUID = data.id;
    window._pollForResult(currentUUID);
  });
});
socket.on('prediction_response', function(data) {
  var pred = parseInt(data.Prediction);
  var msg = delayLabels[pred] || 'Unknown: ' + pred;
  document.getElementById('result').innerHTML = msg;
});