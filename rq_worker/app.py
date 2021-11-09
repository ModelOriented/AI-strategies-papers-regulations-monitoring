import redis
from rq import Connection, Worker
from mars.config import redis_url

redis_connection = redis.from_url(redis_url)

if __name__ == "__main__":
    with Connection(redis_connection):
        worker = Worker("default")
        worker.work()
