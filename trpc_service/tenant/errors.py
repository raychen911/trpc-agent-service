class ControlPlaneError(Exception):
    status_code = 400
    code = "control_plane_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class NotFoundError(ControlPlaneError):
    status_code = 404
    code = "not_found"


class ConflictError(ControlPlaneError):
    status_code = 409
    code = "conflict"


class InvalidStateError(ControlPlaneError):
    status_code = 422
    code = "invalid_state"
