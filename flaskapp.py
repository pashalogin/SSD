from flask import Flask

app = Flask(__name__)
@app.route('/')
def hello_world():
  return 'Hello from Flask!'
@app.route('/countme/<input_str>')
def count_me(input_str):
    return input_str
@app.route('/calculateCost/<claim_id>')
def costFunction(claim_id):
    return claim_id
if __name__ == '__main__':
  app.run()
