"""Handler for recording job statuses in a firestore db."""

from firebase_admin import firestore, initialize_app


class JobStatus:
    """Connect to a firestore db and create/update the status of a job."""

    def __init__(
        self,
        gcp_project_id: str,
        firestore_db_name: str,
        timeout: int | float = 5,
    ):
        self.gcp_project_id = gcp_project_id
        self.firestore_db_name = firestore_db_name
        self.timeout = timeout

        # Initialize Firestore connection
        app_options = {
            "projectId": self.gcp_project_id,
            "httpTimeout": self.timeout,
        }
        app = initialize_app(options=app_options)
        self._db = firestore.client(
            app=app, database_id=self.firestore_db_name
        )

        # non-intrusive test to verify db connection - list all collections
        try:
            _ = list(self._db.collections())
        except Exception as e:
            raise ConnectionError(
                f"Error when connecting to Firestore: {e}"
            ) from e
