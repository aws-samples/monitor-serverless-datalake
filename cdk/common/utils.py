from typing import Dict


def select_artifacts(artifacts: Dict, keep_artifact:str):
    """Remove all other artifacts except for keep_artifact"""
    return [v for k, v in artifacts.items() if k != keep_artifact]
