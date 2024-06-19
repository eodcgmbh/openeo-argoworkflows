import builtins
import json
import keyword
from pathlib import Path

process_json_paths = [
    pg_path for pg_path in (
        Path(__file__).parent
    ).glob("*.json")
]

__all__ = []

for spec_path in process_json_paths:

    spec_json = json.load(open(spec_path))

    process_name = spec_json["id"]
    
    # Make sure we don't overwrite any builtins
    if spec_json["id"] in dir(builtins) or keyword.iskeyword(spec_json["id"]):
        process_name = "_" + spec_json["id"]

    locals()[process_name] = spec_json
    __all__.append(process_name)
