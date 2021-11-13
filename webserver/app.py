from flask import Flask
from mars.config import redis_url
from flask_cors import CORS
import rq_dashboard
import jobs

app = Flask(__name__)
CORS(app)
app.register_blueprint(jobs.blueprint, url_prefix='/api/jobs')
app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")
app.config["RQ_DASHBOARD_REDIS_URL"] = redis_url
app.config['MAX_CONTENT_LENGTH'] = 50 * 1000 * 1000

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
