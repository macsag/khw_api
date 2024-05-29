from pydantic import BaseModel


class UpdaterMessage(BaseModel):
    is_update_in_progress: bool
    last_update_time: str
    message: str
