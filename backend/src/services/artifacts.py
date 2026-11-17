import json,os
from pathlib import Path

def artifacts_dir():
    # Read at call time so tests and deployments can point at a writable volume.
    return Path(os.getenv('ARTIFACTS_DIR','/artifacts'))

def artifact_path(trial_id):
    return artifacts_dir()/f'lsm-trial-{trial_id}.json'

def write_artifact(trial_id,payload):
    path=artifact_path(trial_id)
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.tmp')
    tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2))
    tmp.replace(path)

def remove_artifact(trial_id):
    try:
        artifact_path(trial_id).unlink()
        return True
    except FileNotFoundError:
        return False
