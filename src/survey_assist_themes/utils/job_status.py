"""Handler for recording job statuses in a firestore db."""

import datetime
from dataclasses import dataclass
from enum import Enum

from firebase_admin import firestore, get_app, initialize_app
from google.api_core.exceptions import ServiceUnavailable
from google.api_core.retry import Retry


@dataclass(frozen=True)
class _JobStateDetails:
    """Details properties for job state."""

    msg: str
    start: bool = False
    end: bool = False


class JobState(Enum):
    """Possible states for a job, with associated messages."""

    STARTED = _JobStateDetails("Job has started", start=True)
    IN_PROGRESS = _JobStateDetails("Job is in progress")
    COMPLETED = _JobStateDetails("Job is completed", end=True)
    FAILED = _JobStateDetails("Job has failed", end=True)

    @property
    def msg(self) -> str:
        return self.value.msg

    @property
    def start(self) -> bool:
        return self.value.start

    @property
    def end(self) -> bool:
        return self.value.end


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
        state: JobState,
        start_time: datetime.datetime = None,
        end_time: datetime.datetime = None,
    ):
        """Convert job status information to a dictionary for firestore."""
        now = datetime.datetime.now(tz=datetime.UTC)
        job_status = {
            "user_id": user_id,
            "updated_time": now,
            "state": state.name,
            "msg": state.msg,
        }
        if start_time is not None:
            job_status["start_time"] = start_time
        if end_time is not None:
            job_status["end_time"] = end_time
        return job_status

    def _status_exists(self, job_id) -> bool:
        """Check if a job status document already exists in the db."""
        doc_ref = self._col_ref.document(job_id)
        doc = doc_ref.get(retry=self._retry)
        return doc.exists

    def update(self, state: JobState):
        """Update a job status document in the firestore db.

        Parameters
        ----------
        state : JobState
            The state to update the job status to.

        Raises
        ------
        ValueError
            - If JobState.STARTING is used and a job status document already
            exists (status updates are unique per job).
            - If a non-JobState.STARTING state is used but no job status
            document exists (an initial status update is missing, and this
            would result in missing field information e.g. start_time).
        """
        doc_ref = self._col_ref.document(self._job_id)
        now = datetime.datetime.now(tz=datetime.UTC)

        # these cases handle unintentional misuse of state in upstream logic
        # case start state but status already exists
        if state.start and self._status_exists(self._job_id):
            raise ValueError(
                f"Job status for {self._job_id} already exists but the "
                f"provided state is {state.name}. Expected no existing status "
                "at the job start stage."
            )
        # case non-start state and no existing status
        elif not state.start and not self._status_exists(self._job_id):
            raise ValueError(
                f"No existing job status for {self._job_id} but the provided "
                f"state is {state.name}. Expected an existing status if the "
                f" state is not {JobState.STARTED.name}."
            )

        doc_ref.set(
            self._job_status_to_dict(
                user_id=self._user_id,
                state=state,
                start_time=now if state.start else None,
                end_time=now if state.end else None,
            ),
            merge=True,
            retry=self._retry,
        )
