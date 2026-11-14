# AI Prompt History

This folder holds the prompts given to AI tools while building **DataFlow** for the YottaFlex 72-Hour
Challenge, as the submission guidelines require.

| File | Contents |
|---|---|
| [`prompts.md`](prompts.md) | Every prompt that could be recovered, grouped by development phase, with tool, timestamp, purpose and review notes. |

## Where the prompts come from

| Source | Tool | Period (UTC) | Prompts | Status |
|---|---|---|---|---|
| Claude Code session `062366ba…` (VS Code extension, local transcript in `~/.claude/projects/`) | Claude Code, model `claude-opus-5-5` (Claude Opus 5.5) | 2026-11-12 17:49 → 2026-11-13 05:45 | 6 | **Recovered verbatim** from the session transcript |
| Claude Code session `5f0e144d…` (VS Code extension) | Claude Code, model `claude-opus-5-5` | 2026-11-14 | 1 | **Recovered verbatim**: the prompt for this documentation and submission audit |
| Initial design and implementation, before 2026-11-12 17:49 UTC | **Unknown: not recorded on this machine** | Unknown | Unknown | **Not recovered.** See "Known gaps" |
| Rewrite of `README.md` in commit `fd2ce8d` ("Describe the change", 2026-11-13 13:29 +05:30) | **Unknown** | 2026-11-13 | Unknown | **Not recovered.** See "Known gaps" |

## How the prompts were recovered

Claude Code stores each session as a JSON-lines transcript. The user messages were extracted
programmatically from the transcript file, in order, with no edits. Two kinds of content were left out
because the candidate did not type them:

* `<ide_opened_file>` notes that the VS Code extension adds automatically (for example
  "The user opened the file … dashboard/app.py in the IDE"). Each omission is marked where it happened.
* The `<pasted_content>` wrapper tags. The text inside them is reproduced exactly. These prompts were
  pasted into the chat, which suggests they were drafted somewhere else first. Any tool used to draft
  them is not recorded.

AI responses are optional under the guidelines and are not included. The full transcript, including
responses and every command the assistant ran, is still on the development machine and can be supplied
if a reviewer asks for it.

## Known gaps (the candidate must resolve these before submitting)

1. **Initial build prompts are missing.** The first recovered prompt (2026-11-12 17:49 UTC) begins:
   "I successfully installed all dependencies and verified that Faker works". By then the pipeline,
   generator, dashboard and test suite already existed: the session refers to an existing 100,000-row
   reconciliation and an existing test suite. The prompts that produced that first version were given
   to a tool or session that left no record on this machine (for example claude.ai in a browser, another
   chat assistant, or another computer). **No prompts have been invented to fill this gap.**
   Placeholders are provided in [`prompts.md`](prompts.md#0-not-recovered-initial-planning-design-and-implementation).
2. **README rewrite tool is unknown.** Commit `fd2ce8d` rewrote `README.md` (+1,286 / −391 lines)
   about 40 minutes after the last recovered Claude Code message. No recovered prompt asks for that
   rewrite, so it was made either by hand or with a tool not covered here.
3. **Pasted prompts may have been drafted with AI.** If another AI tool was used to write the prompts
   that were later pasted into Claude Code, those prompts should also be recorded.

If the missing prompts cannot be exported, the candidate should add an honest written summary that is
clearly labelled **"Reconstructed summary — not the original prompt"**, and say which tool was used.
