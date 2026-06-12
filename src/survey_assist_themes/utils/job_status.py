"""Handler for recording job statuses in a firestore db."""

import datetime
from dataclasses import dataclass
from enum import Enum

from firebase_admin import firestore, get_app, initialize_app
from google.api_core.exceptions import ServiceUnavailable
from google.api_core.retry import Retry
from survey_assist_utils.logging import get_logger
from survey_assist_utils.logging.logging_utils import VALID_LOG_LEVELS


@dataclass(frozen=True)
class _JobStateDetails:
    """Job state details.

    Attributes:
        msg: A message describing the job state (human readable).
        start: Whether this state represents the start of a job. Defaults to
            False.
        end: Whether this state represents the end of a job. Defaults to False.
    """

    msg: str
    start: bool = False
    end: bool = False


class JobState(Enum):
    """Possible discrete states for a job."""

    STARTED = _JobStateDetails("Started ThemeFinder job", start=True)
    RUN_ANALYSIS = _JobStateDetails("Running ThemeFinder analysis")
    RUN_POST_PROCESS = _JobStateDetails("Running ThemeFinder post-processing")
    RUN_REPORT = _JobStateDetails("Running ThemeFinder report generation")
    COMPLETED = _JobStateDetails("ThemeFinder job has completed", end=True)
    FAILED = _JobStateDetails("ThemeFinder job has failed", end=True)

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
    """Connect to a firestore db and create/update the status of a job.

    Args:
        gcp_project_id: The GCP project ID where the firestore db is hosted.
        firestore_db_name: The name of the firestore db to connect to.
        job_id: A unique identifier for the job to track the status of.
        user_id: A unique identifier for the user running the job.
        log_level: The logging level to use for the JobStatus class. Must be
            one of "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL". Default is
            "INFO".

    Attributes:
        update: Update the job status in the firestore db with new status
            information. See the method docstring for more details.

    Raises:
        ConnectionError: If there is an error connecting to the firestore db
            during initialisation.

    Examples:
        A typical example usage of the JobStatus class:

            >>> from survey_assist_themes.utils.job_status import JobStatus,
                JobState
            >>> gcp_project_id = "MY_GCP_PROJECT_ID"
            >>> firestore_db_name = "MY_FIRESTORE_DB_NAME"
            >>> job_id = "my_unique_job_id"
            >>> user_id = "my_unique_user_id"
            >>> js = JobStatus(gcp_project_id, firestore_db_name, job_id,
                user_id)
            >>> js.update(JobState.STARTED)
            # Job status document created in firestore db with state "STARTED"
            >>> js.update(JobState.RUN_ANALYSIS)
            # Job status document updated in firestore db with state
            # "RUN_ANALYSIS"
            >>> js.update(JobState.COMPLETED)
            # Job status document updated in firestore db with state
            # "COMPLETED"

        Or, if an error occurs during the job:
            >>> js.update(JobState.FAILED, "A useful error message")
    """

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
        user_id: str,
        log_level: str = "INFO",
    ):
        self._gcp_project_id = gcp_project_id
        self._firestore_db_name = firestore_db_name
        self._job_id = job_id
        self._user_id = user_id
        if log_level not in VALID_LOG_LEVELS:
            raise ValueError(f"log_level must be one of {VALID_LOG_LEVELS}, but got {log_level}.")
        self._logger = get_logger(__name__, log_level)

        # Initialise Firestore connection
        self._logger.debug(
            f"Initialising Firestore connection to {self._firestore_db_name} "
            f"in project {self._gcp_project_id}."
        )
        app_options = {"projectId": self._gcp_project_id}
        try:
            app = initialize_app(options=app_options)
        # handle case where app connection already initialised
        # this can happen during dev when calling __init__ multiple times
        except ValueError as e:
            if "The default Firebase app already exists" in str(e):
                self._logger.warning("Default Firebase app already exists. Using existing app.")
                app = get_app()
            else:
                raise ValueError(f"Error during firestore initialisation: {e}") from e

        self._db = firestore.client(app=app, database_id=self._firestore_db_name)

        # non-intrusive test to verify db connection - list all collections
        try:
            _ = list(self._db.collections(retry=self._retry))
            self._logger.debug("Non-intrusive test to verify db connection succeeded.")
        except Exception as e:
            raise ConnectionError(f"Error when connecting to Firestore: {e}") from e

        self._col_ref = self._db.collection(self._collection_name)
        self._logger.debug(
            f"Successfully connected to Firestore db {self._firestore_db_name}"
            f" in project {self._gcp_project_id}."
        )

    @staticmethod
    def _job_status_to_dict(
        user_id: str,
        state: JobState,
        start_time: datetime.datetime = None,
        end_time: datetime.datetime = None,
        history: list[dict] = None,
        err_msg: str = None,
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
        if history is not None:
            job_status["history"] = history
        if err_msg is not None:
            job_status["err_msg"] = err_msg
        return job_status

    def update(self, state: JobState, err_msg: str = None):
        """Update a job status document in the firestore db.

        A status document will include the following top-level fields:
        - user_id: the user running the job (str)
        - updated_time: the time the status was updated (datetime)
        - state: the current state of the job (str, from JobState enum)
        - msg: a message describing the current state (str, from JobState enum)
        - start_time: the time the job started (datetime, only for start state)

        Subsequent updates to the job status will additionally include:
        - history: a list of previous statuses (with their state, message, and
            update times)

        At the end of the job, the following fields will be added:
        - end_time: the time the job ended (datetime, only for end states)

        Args:
            state: The state to update the job status to.
            err_msg: An optional error message to include in the job status.
                Can only be set when state is JobState.FAILED.

        Raises:
            ValueError: If any of the following conditions are met:
                - JobState.STARTED is provided and a job status document
                already exists (prevents starting a job twice).
                - A non-STARTED state is provided and no job status already
                exists (an initial status update is missing, which would result
                in missing field information like start_time).
                - An end state is provided but the existing job status suggests
                the job has already ended (prevents modifying the status of a
                completed job).
                - An error message is provided when the state is not
                JobState.FAILED (error messages are reserved for failed jobs
                only).
        """
        doc = self._col_ref.document(self._job_id).get(retry=self._retry)
        doc_dict = doc.to_dict() if doc.exists else {}
        now = datetime.datetime.now(tz=datetime.UTC)
        self._logger.debug(f"Provided state for update: {state.name}.")
        self._logger.debug(f"Firestore doc for job {self._job_id} exists: {doc.exists}.")

        # these cases handle unintentional misuse of state in upstream logic
        # case start state but status already exists
        if state.start and doc.exists:
            raise ValueError(
                f"Job status for {self._job_id} already exists and the "
                f"provided state is {state.name}. Expected no existing status "
                "at the job start."
            )
        # case non-start state and no existing status
        elif not state.start and not doc.exists:
            raise ValueError(
                f"No existing job status for {self._job_id} but the provided "
                f"state is {state.name}. Expected an existing status if the "
                f"state is not {JobState.STARTED.name}."
            )
        # case job already ended (don't update/can't end twice).
        elif doc_dict.get("end_time") is not None:
            raise ValueError(
                f"Existing job status for {self._job_id} has an end_time and "
                f"the provided state is {state.name}. Not updating to prevent "
                "modifying the status of a completed job."
            )
        # case not failed and error message is provided (reserve arg)
        elif not state == JobState.FAILED and err_msg is not None:
            raise ValueError(
                f"Provided state is {state.name} but an error message is also "
                "provided. Error messages should only be included for failed "
                "jobs."
            )

        # build and append any existing job status to track history
        if not state.start:
            self._logger.debug("Building job status history for update.")
            history = doc_dict.get("history", [])
            # use direct keys to raise keyerror when required fields missing
            history.append(
                {
                    "state": doc_dict["state"],
                    "msg": doc_dict["msg"],
                    "updated_time": doc_dict["updated_time"],
                }
            )
        else:
            self._logger.debug("No existing job status history to build.")
            history = None

        # upset the job status document with latest state information
        self._logger.debug(f"Updating job status for job {self._job_id} to state {state.name}.")
        self._col_ref.document(self._job_id).set(
            self._job_status_to_dict(
                user_id=self._user_id,
                state=state,
                start_time=now if state.start else None,
                end_time=now if state.end else None,
                history=history,
                err_msg=err_msg,
            ),
            merge=True,
            retry=self._retry,
        )
