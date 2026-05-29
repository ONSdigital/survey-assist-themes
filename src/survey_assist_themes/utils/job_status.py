"""Handler for recording job statuses in a firestore db."""

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
    ):
        self.gcp_project_id = gcp_project_id
        self.firestore_db_name = firestore_db_name

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
