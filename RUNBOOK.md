# Dev runbook

## After running the test suite

The test fixtures drop all tables on teardown. If the dev backend is running
against the same DB (`aidash_test` on port 5433), you need to recreate the
schema and re-seed after any test run:

```bash
source .venv/bin/activate
python -c "
import asyncio
from app.auth.models import Base
from app.database import engine
async def create():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
asyncio.run(create())
"
python -m app.auth.seed
```
