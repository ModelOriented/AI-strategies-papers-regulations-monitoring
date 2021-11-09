from flask import Flask
from mars.config import redis_url
import rq_dashboard
import jobs

app = Flask(__name__)
app.register_blueprint(jobs.blueprint, url_prefix='/api/jobs')
app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")
app.config["RQ_DASHBOARD_REDIS_URL"] = redis_url

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
