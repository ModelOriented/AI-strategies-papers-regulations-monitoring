import os
import string
import random
import hashlib
import logging

from mars import config

from webdav3.client import Client, wrap_connection_error
from webdav3.exceptions import RemoteResourceNotFound, ResponseErrorCode
from webdav3.urn import Urn

logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(config.logging_level))


class FixedWebDavClient(Client):
    """
    This class modifies default webdav3 client to support servers,
    that do not allow GET or HEAD on directories
    """

    @wrap_connection_error
    def check(self, remote_path="/"):
        urn = Urn(remote_path)
        try:
            response = self.execute_request(action="info", path=urn.quote())
        except RemoteResourceNotFound:
            return False
        except ResponseErrorCode:
            return False
        if int(response.status_code) in [200, 207]:
            return True
        return False


def get_random_filename(length=30):
    return "".join(random.choice(string.ascii_letters) for i in range(length))


class FileSync:
    """
    Class for operations on remote and local files based on
    synchronization at the beginning and the end of 'with'
    statement.

    Examples
    --------
    with FileSync('test_file_id') as path:
        with open(path, 'w') as file:
            file.write('test content')
    """

    def __init__(self, file_id, options={}):
        self.file_id = file_id
        # Hash as low level file name resolves issue of special characters in file_id
        self.file_id_hash = hashlib.sha256(str(file_id).encode("UTF-8")).hexdigest()

        # Overwrite default config with options
        conf = {**config.__dict__, **options}

        self.use_webdav = conf.get("use_webdav")
        self.lock = False

        if self.use_webdav:
            self.webdav_options = {
                "webdav_hostname": conf.get("webdav_url"),
                "webdav_login": conf.get("webdav_login"),
                "webdav_password": conf.get("webdav_password"),
            }
            self.sync_dir = conf.get("tmp_files_dir").rstrip("/")
            self.client = FixedWebDavClient(self.webdav_options)
            try:
                self.client.check("/")
            except:
                logger.error("Webdav authorization failed or server is not supported")
        else:
            self.local_dir = conf.get("raw_files_dir").rstrip("/")

    def __enter__(self):
        if self.lock:
            raise Exception("This object can be used only once")
        self.lock = True

        if not self.use_webdav:
            return self.local_dir + "/" + self.file_id_hash

        # Create sync dir
        if not os.path.exists(self.sync_dir):
            os.makedirs(self.sync_dir)

        # Temporary path in local filesystem that will be synced with remote file
        self.sync_file_path = self.sync_dir + "/" + get_random_filename()

        # If file exists remotely
        try:
            self.client.download_sync(
                remote_path=self.file_id_hash, local_path=self.sync_file_path
            )
        except RemoteResourceNotFound:
            pass

        self.last_modification_time = (
            os.path.getmtime(self.sync_file_path)
            if os.path.exists(self.sync_file_path)
            else -1
        )

        return self.sync_file_path

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is not None:
            raise Exception("Failed to save file")
        elif self.use_webdav:
            # If file exists remotely, but not localy then remove remote file
            if not os.path.exists(self.sync_file_path) and self.client.check(
                self.file_id_hash
            ):
                self.client.clean(self.file_id_hash)
            # File exists localy
            elif (
                os.path.isfile(self.sync_file_path)
                and os.path.getmtime(self.sync_file_path) > self.last_modification_time
            ):
                self.client.upload_sync(
                    remote_path=self.file_id_hash, local_path=self.sync_file_path
                )
        if self.use_webdav and os.path.exists(self.sync_file_path):
            os.unlink(self.sync_file_path)
