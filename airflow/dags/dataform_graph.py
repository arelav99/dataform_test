import re
from dataclasses import dataclass
from pathlib import Path

CONFIG_RE = re.compile(r"\bconfig\s*\{(.*?)\}", re.DOTALL)
FIELD_RE = re.compile(r'(\w+)\s*:\s*"([^"]*)"')
TAGS_RE = re.compile(r"\btags\s*:\s*\[([^\]]*)\]")
REF_RE = re.compile(r"\$\{\s*ref\(([^)]*)\)\s*\}")
STRING_RE = re.compile(r'"([^"]*)"')


@dataclass(frozen=True)
class Action:
    name: str
    schema: str
    tags: tuple[str, ...]
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
        config_text = match.group(1) if match else ""
        config = dict(FIELD_RE.findall(config_text))
        tags_match = TAGS_RE.search(config_text)
        tags = tuple(STRING_RE.findall(tags_match.group(1))) if tags_match else ()
        name = config.get("name", path.stem)
        if "schema" not in config:
            raise ValueError(f"{path}: config must set schema")
        if name in definitions:
            raise ValueError(f"{path}: duplicate action name '{name}'")
        definitions[name] = {
            "path": path,
            "schema": config["schema"],
            "tags": tags,
            "declaration": config.get("type") == "declaration",
            "refs": [_ref_name(args, path) for args in REF_RE.findall(text)],
        }

    if not definitions:
        raise ValueError(f"no Dataform definitions found in {definitions_dir}")

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
        actions[name] = Action(
            name=name,
            schema=definition["schema"],
            tags=definition["tags"],
            upstream=upstream,
        )
    return actions


def layers_in_order(actions: dict[str, Action]) -> list[str]:
    """Return the tags of the actions, ordered so that each layer follows the layers it reads from.

    A layer is a tag. Every action must carry at least one tag, so no action is skipped
    when the layers run.
    """
    untagged = [action.name for action in actions.values() if not action.tags]
    if untagged:
        raise ValueError(f"actions without tags cannot run in any layer: {', '.join(untagged)}")

    layers = {tag for action in actions.values() for tag in action.tags}
    reads_from = {layer: set() for layer in layers}
    for action in actions.values():
        for parent_name in action.upstream:
            for layer in action.tags:
                reads_from[layer].update(
                    parent_tag for parent_tag in actions[parent_name].tags if parent_tag != layer
                )

    order = []
    remaining = reads_from
    while remaining:
        ready = sorted(layer for layer, parents in remaining.items() if not parents)
        if not ready:
            raise ValueError(f"layers depend on each other in a cycle: {', '.join(sorted(remaining))}")
        order.extend(ready)
        remaining = {
            layer: parents - set(ready)
            for layer, parents in remaining.items()
            if layer not in ready
        }
    return order
