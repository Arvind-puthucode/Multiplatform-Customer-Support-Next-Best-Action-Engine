
from pydantic import BaseModel, Field
from typing import Dict, Optional
from datetime import datetime
import uuid

class UniversalInteraction(BaseModel):
    interaction_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    external_id: str
    platform_type: str
    participant_external_id: str
    content_text: str
    content_metadata: Optional[Dict] = None
    interaction_timestamp: datetime
    parent_interaction_id: Optional[str] = None
    interaction_type: str
    platform_metadata: Optional[Dict] = None
    processing_metadata: Optional[Dict] = None
