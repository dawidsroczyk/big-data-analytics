import asyncio
import random
from datetime import datetime
from typing import Dict, Any
from typing import Dict, Any
import asyncio
import random
from datetime import datetime

class MockUVClient:
    """Mock UV data for development/testing"""

    def __init__(self, config):
        self.config = config

    async def get_uv_index(self, lat: float, lng: float) -> Dict[str, Any]:
        await asyncio.sleep(0.1)
        now = datetime.utcnow()
        return {
            "uv_index": round(random.uniform(0, 12), 2),
            "location": f"{lat},{lng}",
            "timestamp": now.isoformat(),
            "dt": int(now.timestamp()),
            "provider": "mock",
        }
