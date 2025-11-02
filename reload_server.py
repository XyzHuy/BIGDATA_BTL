from flask import Flask, jsonify
import subprocess, requests  

app = Flask(__name__)
CONTAINER_FLASK_URL = "http://host.docker.internal:5000/reload-done-internal"

@app.route('/trigger-reload', methods=['POST'])
def trigger_reload():
    subprocess.Popen(['bash', 'reload.sh'])
    return {"status": "ok"}

@app.route('/reload-done', methods=['POST'])
def reload_done():
    print("[Host] Reload finished — notifying Flask in container...")
    try:
        requests.post(CONTAINER_FLASK_URL, json={"status": "done"})
        return jsonify({"status": "ok"})
    except Exception as e:
        print(f"[Host] Failed to notify container: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    print("Starting Flask reload server...")
    app.run(port=6000, host='0.0.0.0')
