from flask import Flask, render_template

app = Flask(__name__)

@app.route("/")
def login():
    return render_template("/home/cdsw/Efficient_Model_Development_CML/example.html")
  
if __name__ == '__main__':
    app.debug = True
    app.run(host='127.0.0.1', port=int(os.environ['CDSW_READONLY_PORT']))