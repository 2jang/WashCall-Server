from enum import Enum
from pydantic import BaseModel
from typing import Optional, List
import time

class StatusEnum(str, Enum):
    WASHING = "WASHING"
    SPINNING = "SPINNING"
    DRYING = "DRYING"
    FINISHED = "FINISHED"
    EXT_VIBE = "EXT_VIBE"
    OFF = "OFF"

# /update용 스키마
class UpdateData(BaseModel):
    machine_id: int
    secret_key: str
    status: StatusEnum
    machine_type: str
    timestamp: int
    battery: Optional[int] = None
    wash_avg_magnitude: float = None  # FINISHED일 때만
    wash_max_magnitude: float = None
    spin_max_magnitude: float = None
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "machine_id": 5,
                    "secret_key": "string",
                    "status": "WASHING",
                    "machine_type": "washer",
                    "timestamp": int(time.time()),  # 현재 타임스탬프
                    "battery": 0,
                    "wash_avg_magnitude": 0,
                    "wash_max_magnitude": 0,
                    "spin_max_magnitude": 0
                }
            ]
        }
    }

# /device_update용 스키마
class DeviceUpdateRequest(BaseModel):
    machine_id: int
    timestamp: int
    secret_key: str
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "machine_id": 5,
                    "timestamp": int(time.time())  # 현재 타임스탬프
                }
            ]
        }
    }

class DeviceUpdateResponse(BaseModel):
    message: str = "received"
    NewWashThreshold: float = None  # 컬럼명 변경
    NewSpinThreshold: float = None  # 컬럼명 변경


class DeviceRegisterRequest(BaseModel):
    machine_id: int


class DeviceRegisterResponse(BaseModel):
    message: str = "ok"
    registered: bool = False
    token: str = ""


# /raw_data용 스키마 (배치 + 델타 기반)
class RawSample(BaseModel):
    timestamp: int
    deltaX: float
    deltaY: float
    deltaZ: float
    gyroDeltaX: float
    gyroDeltaY: float
    gyroDeltaZ: float


class RawDataBatchRequest(BaseModel):
    machine_id: int
    secret_key: str
    samples: List[RawSample]
     
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "machine_id": 5,
                    "secret_key": "string",
                    "samples": [
                        {
                            "timestamp": int(time.time()),
                            "deltaX": 0.1,
                            "deltaY": 0.2,
                            "deltaZ": 0.3,
                            "gyroDeltaX": 1.0,
                            "gyroDeltaY": 2.0,
                            "gyroDeltaZ": 3.0
                        }
                    ]
                }
            ]
        }
    }


class RawDataCompactRequest(BaseModel):
    machine_id: int
    secret_key: str
    t0: int
    dt: int
    samples: List[List[int]]

 
class RawDataResponse(BaseModel):
    message: str = "receive ok"
    inserted: int = 0
