import re
from dataclasses import dataclass
from pathlib import Path

CONFIG_RE = re.compile(r"\bconfig\s*\{(.*?)\}", re.DOTALL)
FIELD_RE = re.compile(r'(\w+)\s*:\s*"([^"]*)"')
REF_RE = re.compile(r"\$\{\s*ref\(([^)]*)\)\s*\}")
STRING_RE = re.compile(r'"([^"]*)"')


@dataclass(frozen=True)
class Action:
    name: str
    schema: str
    upstream: tuple[str, ...]


def _ref_name(args: str, path: Path) -> str:
    strings = STRING_RE.findall(args)
    if not strings:
        raise ValueError(f"{path}: ref() must use string literals, got ref({args})")
    return strings[-1]


def load_actions(definitions_dir: str) -> dict[str, Action]:
    """Return one Action per table/view/operation in definitions_dir, keyed by name.

    Declarations are sources, not tasks, so they are not returned. Their names are
    still resolved, so a ref() to a declaration is valid but adds no dependency.
    """
    definitions = {}
    for path in sorted(Path(definitions_dir).rglob("*.sqlx")):
        text = path.read_text()
        match = CONFIG_RE.search(text)
        config = dict(FIELD_RE.findall(match.group(1))) if match else {}
        name = config.get("name", path.stem)
        if "schema" not in config:
            raise ValueError(f"{path}: config must set schema")
        if name in definitions:
            raise ValueError(f"{path}: duplicate action name '{name}'")
        definitions[name] = {
            "path": path,
            "schema": config["schema"],
            "declaration": config.get("type") == "declaration",
            "refs": [_ref_name(args, path) for args in REF_RE.findall(text)],
        }

    actions = {}
    for name, definition in definitions.items():
        if definition["declaration"]:
            continue
        for ref in definition["refs"]:
            if ref not in definitions:
                raise ValueError(f"{definition['path']}: ref('{ref}') matches no action")
        upstream = tuple(
            dict.fromkeys(ref for ref in definition["refs"] if not definitions[ref]["declaration"])
        )
        actions[name] = Action(name=name, schema=definition["schema"], upstream=upstream)
    return actions
