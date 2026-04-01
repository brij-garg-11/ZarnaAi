"""
Zarna Operator Dashboard — standalone Flask service.
Deploy as a separate Railway service pointing at the same DATABASE_URL.
No PII is ever returned to the frontend — phone numbers are masked or counted only.
"""

import os
from dotenv import load_dotenv
load_dotenv()

from app import create_app

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
