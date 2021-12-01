import redis
from mars.db import collections
from mars.db import db_fields
from rq import Queue, Connection
from mars.config import redis_url

redis_connection = redis.from_url(redis_url)

def run_segmentation(doc_key):
    from mars.segmentation.segmentation import segment_and_upload
    segment_and_upload(doc_key, doc_key, False)

def run_splitting(doc_key):
    from mars.sentences_splitting import split_to_sentences
    split_to_sentences(doc_key, doc_key)

def run_document_definition_scoring(doc_key):
    from mars.document_definition_scoring import document_definition_scoring
    document_definition_scoring(doc_key, doc_key)

def run_sentence_embedding(doc_key):
    from mars.sentence_embeddings import score_embeddings_for_documents
    score_embeddings_for_documents(doc_key, doc_key)

def run_issues_scoring(doc_key):
    from mars.similarity_calculation import infer_issues_for_documents
    infer_issues_for_documents()
    # TODO dokończ
    pass

steps = [
        { 'name': 'Segmentation', 'method': run_segmentation },
        { 'name': 'Splitting to sentences', 'method': run_splitting },
        { 'name': 'Definition scoring', 'method': run_document_definition_scoring },
        { 'name': 'Calculating embeddings', 'method': run_sentence_embedding }
]



def report_success(job, connection, result, *args, **kwargs):
    doc_key = str(job.args[0])
    doc = collections.document_sources.fetchFirstExample({'_key': doc_key})[0]
    for step in doc[db_fields.DOC_JOBS]:
        if step['job'] == job.get_id():
            step['status'] = 'done'
    doc.save()

def report_failure(job, connection, type, value, traceback):
    doc_key = str(job.args[0])
    doc = collections.document_sources.fetchFirstExample({'_key': doc_key})[0]
    for step in doc[db_fields.DOC_JOBS]:
        if step['job'] == job.get_id():
            step['status'] = 'failed'
    doc.save()

def setup_jobs(doc_key):
    doc = collections.document_sources.fetchFirstExample({'_key': str(doc_key)})[0]
    if doc is None:
        raise Exception('Invalid doc key')
    doc[db_fields.DOC_JOBS] = []
    with Connection(redis_connection):
        q = Queue()
        task = None
        for step in steps:
            task = q.enqueue(step['method'], int(doc_key), depends_on=[] if task is None else task, on_success=report_success, on_failure=report_failure, job_timeout=30 * 60)
            doc[db_fields.DOC_JOBS].append({
                'name': step['name'],
                'status': 'waiting',
                'job': task.get_id()
            })
    doc.save()

def get_jobs_status(doc_key):
    doc = collections.document_sources.fetchFirstExample({'_key': str(doc_key)})[0]
    if doc is None:
        raise Exception('Invalid doc key')
    steps = doc[db_fields.DOC_JOBS]

    with Connection(redis_connection):
        q = Queue()
        for step in steps:
            job = q.fetch_job(step['job'])
            if job is not None:
                status = job.get_status()
                if status == 'started':
                    step['status'] = 'running'
    return steps
