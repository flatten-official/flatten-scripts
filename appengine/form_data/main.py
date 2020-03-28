import datetime

from flask import Flask, Response
from service import main

app = Flask(__name__)

@app.route('/')
def root():
    try:
        main()
        return Response(status=200)
    except Exception as e:
        print(e)
        return Response(status=500)

# for local testing
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)