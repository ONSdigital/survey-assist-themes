"""Handler for recording job statuses in a firestore db."""

import datetime

from firebase_admin import firestore, get_app, initialize_app
from google.api_core.exceptions import ServiceUnavailable
from google.api_core.retry import Retry


class JobStatus:
    """Connect to a firestore db and create/update the status of a job."""

    _collection_name = "job_status"
    _retry = Retry(
        predicate=lambda exc: isinstance(exc, ServiceUnavailable),
        initial=0.5,
        maximum=10.0,
        multiplier=1.5,
        deadline=30.0,
    )

    def __init__(
        self,
        gcp_project_id: str,
        firestore_db_name: str,
        job_id: str,
        user_id: str = None,
    ):
        self.gcp_project_id = gcp_project_id
        self.firestore_db_name = firestore_db_name
        self._job_id = job_id
        self._user_id = user_id

        # Initialize Firestore connection
        app_options = {"projectId": self.gcp_project_id}
        try:
            app = initialize_app(options=app_options)
        # handle case where app connection already initialised
        # this can happen during dev when calling __init__ multiple times
        except ValueError as e:
            if "The default Firebase app already exists" in str(e):
                app = get_app()
            else:
                raise ValueError(
                    f"Error during firestore initialisation: {e}"
                ) from e

        self._db = firestore.client(
            app=app, database_id=self.firestore_db_name
        )

        # non-intrusive test to verify db connection - list all collections
        try:
            _ = list(self._db.collections(retry=self._retry))
        except Exception as e:
            raise ConnectionError(
                f"Error when connecting to Firestore: {e}"
            ) from e

        self._col_ref = self._db.collection(self._collection_name)

    @staticmethod
    def _job_status_to_dict(
        user_id: str,
        status: str,
        msg: str = "",
        start_time: datetime.datetime = None,
        end_time: datetime.datetime = None,
    ):
        """Convert job status information to a dictionary for firestore."""
        now = datetime.datetime.now(tz=datetime.UTC)
        job_status = {
            "user_id": user_id,
            "updated_time": now,
            "status": status,
            "msg": msg,
        }
        if start_time is not None:
            job_status["start_time"] = start_time
        if end_time is not None:
            job_status["end_time"] = end_time
        return job_status

    def create(self, status: str, msg: str = ""):
        """Create a new job status document in the firestore db."""
        doc_ref = self._col_ref.document(self._job_id)
        start_time = datetime.datetime.now(tz=datetime.UTC)
        doc_ref.set(
            self._job_status_to_dict(
                user_id=self._user_id,
                status=status,
                msg=msg,
                start_time=start_time,
            ),
            retry=self._retry,
        )

    def update(self, status: str, msg: str = "", job_end: bool = False):
        """Update an existing job status document in the firestore db."""
        doc_ref = self._col_ref.document(self._job_id)
        end_time = datetime.datetime.now(tz=datetime.UTC) if job_end else None
        doc_ref.update(
            self._job_status_to_dict(
                user_id=self._user_id,
                status=status,
                msg=msg,
                end_time=end_time,
            ),
            retry=self._retry,
        )
