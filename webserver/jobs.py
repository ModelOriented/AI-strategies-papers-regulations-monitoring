import json
from flask import Blueprint, request, Response, abort
from mars.db.db import save_doc
from mars.db.db_fields import FileType, SourceWebsite, FILENAME as doc_field_filename, DOC_JOBS, USER as DOC_USER, DOC_NAME
from mars.db import collections
from mars.storage import FileSync
from mars.jobs import setup_jobs, get_jobs_status

blueprint = Blueprint('jobs', __name__)

@blueprint.route('/<string:key>/status')
def get_status(key):
    try:
        return Response(json.dumps(get_jobs_status(key)))
    except:
        abort(400)

@blueprint.route('/', methods=["POST"])
def upload_document():
    f = request.files.get('file')
    if f is None:
        return Response(json.dumps({'error': "File is missing"}), 400)
    if f.mimetype not in ['application/pdf', 'text/html']:
        return Response(json.dumps({'error': "File type is need to be PDF or HTML"}), 400)

    name = request.form.get('name')
    if name is None:
        return Response(json.dumps({'error': "Name is missing"}), 400)
    if not isinstance(name, str):
        return Response(json.dumps({'error': "Name is invalid"}), 400)
    if len(name) > 120:
        return Response(json.dumps({'error': "Name is too long"}), 400)
    if len(name) < 5:
        return Response(json.dumps({'error': "Name is too short"}), 400)

    filetypes = {'application/pdf': FileType.pdf, 'text/html': FileType.html}
    doc = save_doc(None, None, filetypes[f.mimetype], SourceWebsite.manual, {}, 'web_1234', name)

    with FileSync(doc[doc_field_filename]) as path:
        f.save(path)

    setup_jobs(doc['_key'])
    return { 'document': doc['_key'] }

@blueprint.route('/', methods=["GET"])
def list_documents():
    user_id = 'web_1234'
    docs = collections.document_sources.fetchByExample({DOC_USER: user_id}, batchSize=1000)
    docs_list = []
    for doc in docs:
        status = 'Processing'
        is_done = all(step['status'] == 'done' for step in doc[DOC_JOBS])
        is_failed = any(step['status'] == 'failed' for step in doc[DOC_JOBS])
        if is_done:
            status = 'Done'
        elif is_failed:
            status = 'Failed'
        docs_list.append({'status': status, 'name': doc[DOC_NAME], 'key': doc['_key']})
    return Response(json.dumps(docs_list))

@blueprint.route('/<string:key>', methods=['DELETE'])
def delete_document(key):
    user_id = 'web_1234'
    docs = list(collections.document_sources.fetchFirstExample({DOC_USER: user_id, '_key': str(key)}))
    if len(docs) == 0:
        return abort(404)
    doc = docs[0]
    is_done = all(step['status'] == 'done' for step in doc[DOC_JOBS])
    is_failed = any(step['status'] == 'failed' for step in doc[DOC_JOBS])
    if not (is_done or is_failed):
        return abort(403)
    doc.delete()
    return ""
