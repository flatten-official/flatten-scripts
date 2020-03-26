import base64
from flask import Flask, request
import os
import sys
import imports
import warnings

subfolders = [f.path for f in os.scandir('.') if f.is_dir()]

run = {}

for folder in subfolders:
    script = os.path.join(folder, "script.py")
    print(script)
    name = os.path.basename(folder)
    if os.path.isfile(script):
        run[name] = imports.path_import(script)

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():
    envelope = request.get_json()
    if not envelope:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        name = base64.b64decode(pubsub_message['data']).decode('utf-8').strip()
    else:
        return ('', 400) # error

    print(pubsub_message)

    if name in run:
        print(f'Running {name}')
        try:
            run[name].main(pubsub_message['attributes'] if 'attributes' in pubsub_message else None)
        except Exception as e:
            # failure
            print(e)
            return f'Error in instance: {e}', 400
    else:
        print(f'No such name {name} in modules to be run - pubsub message incorrect.')

    # Flush the stdout to avoid log buffering.
    sys.stdout.flush()

    return ('', 204)


if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080


    app.run(host='127.0.0.1', port=PORT, debug=True)