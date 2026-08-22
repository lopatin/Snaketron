# Agent and factory integration

`skills/author-skin/` is canonical. The factory loads this directory explicitly,
validates it with `scripts/validate_package.py`, hashes the complete package,
and includes the pinned bundle in `WorkerRequest`. Production correctness must
not depend on ambient skill discovery.

Thin repository wrappers exist for interactive tools:

- Claude-compatible: `.claude/skills/author-skin/SKILL.md`
- Codex/Agent Skills-compatible: `.agents/skills/author-skin/SKILL.md`
- Hermes operator environments: `hermes/skills/author-skin/SKILL.md`

Each wrapper instructs the agent to load this canonical file and has no
authoring rules of its own. If a host cannot resolve cross-directory references,
install or bundle the canonical directory itself at that host's normal skill
root. Do not copy the content and let it drift.

The skill has no Claude, Codex, Hermes, Gemini SDK, or shell initialization
dependency. A factory task worker returns structured requests; the driver owns
all external actions. An interactive agent may execute those actions only when
the user has authorized them and must still use the same schemas and gates.

Repository maintainers can validate the package and all four route fixtures
with:

```bash
python3 skills/author-skin/scripts/validate_package.py --cargo
python3 -m unittest discover -s skills/author-skin/tests -v
```

Use `--candidate-playbook PATH` on the first command before accepting a GEPA
candidate; it rejects changes outside the one marked playbook body.
