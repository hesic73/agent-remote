# CLAUDE.md

## Code Style

* **No Emojis**: Do not use emojis in code unless explicitly requested.

## Documentation

* **No Unsolicited Markdown**: Do not create markdown documents unless explicitly requested (e.g., if asked to summarize next steps in a document).
* **No Module Docstrings**: Do not add top-level (module) docstrings.
* **Minimal Docstrings**: Keep docstrings short and only when they add information the signature does not already convey (non-obvious contract, side effect, units, edge case). Trivial docstrings that paraphrase the function name or signature must be removed, not written.
* **No Filler Text**: Do not add comments, UI copy, logs, or errors that merely restate adjacent code or widgets; leave the surface empty unless the text adds non-obvious information.

## Error Handling

* **No Silent Failures**: NEVER use silent fallback. Prefer explicit configuration for property values. If a required value is missing, raise an error or crash immediately; do not attempt to use a default value and ignore the issue.

## Design Priority

* **Simplicity First**: Favor **simplicity first**, then **human readability**, then **correctness for edge and complex cases**, over everything else. Core functionality must be correct, but do not introduce complexity solely to handle rare or theoretical edge cases. Apply **KISS**: keep implementations simple and easy for humans to understand; avoid cleverness unless it clearly improves simplicity.
