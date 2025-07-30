import os

def is_dry_run_enabled():
    return os.getenv("DRY_RUN", "false").lower() == "true"

def log_dry_run_action(action):
    print(f"[DRY RUN] {action}")
