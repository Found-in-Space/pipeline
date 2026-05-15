from __future__ import annotations

import os
from typing import Any

GAIA_CREDENTIALS_MESSAGE = (
    "GAIA_CREDENTIALS_FILE or GAIA_USER plus GAIA_PASS"
)


def login_gaia_from_environment_if_available(gaia: Any) -> bool:
    """Log in to Gaia Archive when unattended credentials are configured."""
    credentials_file = os.environ.get("GAIA_CREDENTIALS_FILE")
    if credentials_file:
        gaia.login(credentials_file=credentials_file)
        return True

    user = os.environ.get("GAIA_USER")
    password = os.environ.get("GAIA_PASS")
    if user and password:
        gaia.login(user=user, password=password)
        return True

    return False
