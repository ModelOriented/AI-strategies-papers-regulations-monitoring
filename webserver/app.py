from flask import Flask, send_from_directory
from mars.config import redis_url
from flask_cors import CORS

# import rq_dashboard
# import jobs
import documents

app = Flask(__name__)
CORS(app)
# app.register_blueprint(jobs.blueprint, url_prefix='/api/jobs')
app.register_blueprint(documents.blueprint, url_prefix="/api/documents")
# app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")
app.config["RQ_DASHBOARD_REDIS_URL"] = redis_url
app.config["MAX_CONTENT_LENGTH"] = 50 * 1000 * 1000


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def index(path):
    return app.send_static_file("index.html")


@app.route("/js/<path:path>")
def send_js(path):
    return app.send_static_file("js/" + path)


@app.route("/css/<path:path>")
def send_css(path):
    return app.send_static_file("css/" + path)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
