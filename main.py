from flask import Flask, jsonify
import os

# Flask 앱 생성 (이 이름이 매우 중요: app)
app = Flask(__name__)

# 헬스 체크 엔드포인트
@app.route("/", methods=["GET"])
def health():
    return "OK", 200


# 실제 작업 트리거용 (나중에 Scheduler가 호출)
@app.route("/run", methods=["POST", "GET"])
def run_job():
    return jsonify({
        "status": "running",
        "message": "Cloud Run is working"
    }), 200


# 로컬 실행용 (Cloud Run에서는 gunicorn이 실행)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
