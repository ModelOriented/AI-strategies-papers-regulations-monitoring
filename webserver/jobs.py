import redis
from rq import Queue, Connection
from flask import Blueprint
from mars.config import redis_url
from mars.segmentation.segmentation import segment_and_upload

blueprint = Blueprint('jobs', __name__)

redis_connection = redis.from_url(redis_url)

@blueprint.route('/test')
def list_jobs():
    with Connection(redis_connection):
        q = Queue()
        task = q.enqueue(segment_and_upload, 10, 2000)
    return task.get_id()
