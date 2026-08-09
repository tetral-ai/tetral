/**
 * @packageDocumentation
 * Holds the provider-visible builtin tool copy approved as one unit. It guards the correspondence
 * between documented tool names, parameter labels, and the schemas materialized by tool-catalog;
 * a missing parameter description fails instead of silently producing incomplete provider copy.
 * Tool-catalog calls the typed description accessors while constructing definitions. This module
 * reads only its immutable copy table and calls no Runtime, Gateway, Bridge, or sandbox service.
 */
// Provider-visible copy approved as one unit; keep wording synchronized with the tool census.
export const BuiltinToolCopy = {
  "Bash": {
    "description": "Executes a bash command in the session's sandbox and returns its stdout,\nstderr, and exit code.\n\nCommands run non-interactively (no TTY). Each call is a fresh shell, so\nshell state — environment variables, functions, and the working\ndirectory — does not persist between calls; use absolute paths, or set\n`cwd` per call, rather than relying on a previous `cd`. Quote paths that\ncontain spaces with double quotes.\n\nPrefer dedicated tools over shell equivalents — they are safer and\neasier to review: Read (not cat/head/tail), Edit (not sed/awk), Write\n(not echo >/heredoc), Grep (not grep/rg), Glob (not find/ls). Reserve\nBash for running programs, builds, tests, git, and package managers.\n\nRun independent commands in parallel by sending multiple Bash calls in\none message; chain dependent steps with `&&` in a single call. For\nlong-running work, set `run_in_background: true` — you are notified when\nit completes, so do not poll for it.\n\nOutput is bounded to roughly 50 KiB or 2000 lines per stream; beyond\nthat it is truncated with byte and line counts. A non-zero exit code is\nreturned alongside the output, not raised as a tool error. May pause for\nuser approval before it runs.",
    "parameters": {
      "command (required)": "The shell command to run. Executed with\n/bin/bash -c as a non-interactive shell that does not source login or\nrc profiles.",
      "cwd": "Working directory for this one command, confined to the session\nsandbox. Defaults to the sandbox workspace root.",
      "timeout": "Maximum run time in milliseconds. Default 120000; range\n0-600000. A foreground command reaching it is killed; for a background\ncommand it bounds total lifetime.",
      "run_in_background": "Default false. When true, the command detaches\nalmost immediately, returns initial output, and completion is\ndelivered back automatically when it finishes."
    }
  },
  "Read": {
    "description": "Reads a file from the session sandbox; a missing path returns an error.\n- `file_path` must be an absolute path.\n- Returns up to 2000 lines from the start by default; a larger `limit`\n  clamps to 2000 rather than erroring.\n- `offset` is the 1-indexed start line; call again with a larger offset\n  for later sections.\n- Each returned line is prefixed by its line number. Any line longer\n  than 2000 characters is cut with a \"… [line truncated]\" marker, and\n  the window is byte-bounded at roughly 200,000 bytes; a cut-short\n  window is flagged truncated and reports the next offset to continue\n  from.\n- Reading a directory errors — use Glob to list filenames and Grep to\n  find content in large files.\n- Images (PNG/JPEG/GIF/WebP) and PDFs return as attachments, not text;\n  set `page_range` to select PDF pages.\n- Read files in parallel when you know you need several.",
    "parameters": {
      "file_path (required)": "Absolute path of the file to read.",
      "offset": "1-indexed line to start from. Default 1.",
      "limit": "Maximum lines to return. Default 2000; clamps to 2000. The\nwindow also stops at roughly 200,000 bytes.",
      "page_range": "PDF only. One contiguous range of at most 5 pages (\"3\" or\n\"2-6\"); defaults to 1-5. A range entirely beyond the document errors."
    }
  },
  "Write": {
    "description": "Writes a file to the session sandbox.\n- Overwrites the existing file at `file_path` if one is there, and\n  creates parent directories as needed.\n- Prefer editing existing files with Edit; create new files only when\n  required.\n- Never proactively create documentation (*.md) or README files unless\n  the user explicitly asks.\n- Only write emojis if the user explicitly requests them.\n- Writes are atomic: a crash leaves either the whole old file or the\n  whole new file, never a torn mix. A directory at the path, or an\n  unwritable path, returns an error.\nMay pause for user approval before it runs.",
    "parameters": {
      "file_path (required)": "Absolute path to write. An existing file's\npermission mode is preserved; a new file is created 0644.",
      "content (required)": "Full file contents. Replaces the entire file."
    }
  },
  "Edit": {
    "description": "Performs exact string replacements in a session-sandbox file.\n- `old_string` must match the file's current contents exactly,\n  including indentation. Read the file first to copy the exact text —\n  line-number prefixes are display only, never include them.\n- The edit FAILS with \"old_string was not found\" when `old_string` is\n  absent, and with \"old_string matched more than once\" when it is not\n  unique; add more surrounding context to make it unique, or set\n  `replace_all` to change every occurrence.\n- `old_string` and `new_string` must differ; empty `old_string` is\n  rejected.\n- Use `replace_all` to rename a symbol throughout the file.\n- If exact matching finds nothing, one fallback pass maps curly quotes\n  to their ASCII forms and retries; there is no whitespace fallback.\n  The file must be UTF-8 and under 32 MiB.\nMay pause for user approval before it runs.",
    "parameters": {
      "file_path (required)": "Absolute path of an existing regular file.",
      "old_string (required)": "Exact text to replace.",
      "new_string (required)": "Replacement text. Must differ from old_string.",
      "replace_all": "Replace every occurrence instead of requiring a single\nunique match. Default false; multiple matches are rejected as\nambiguous (the error reports the count)."
    }
  },
  "Grep": {
    "description": "Fast content search over the session sandbox, powered by ripgrep.\n- Full regex syntax (e.g. \"log.*Error\", \"func\\s+\\w+\"); respects\n  .gitignore and skips VCS dirs but does search hidden files.\n- `mode` selects output: \"files\" (default) returns matching file paths,\n  newest-modified first; \"content\" returns matching lines; \"count\"\n  returns per-file match counts.\n- Narrow with `globs` (e.g. [\"*.ts\",\"*.tsx\"]), `file_type` (e.g. \"py\"),\n  or `path` (a directory to search under).\n- In content mode, `line_numbers` (default on) and context parameters\n  add surrounding lines; `multiline` lets a pattern span lines.\n- Results are capped by `head_limit` rows (default 250, max 1000) plus\n  byte/line budgets; large results are flagged truncated. Use `offset`\n  to page files-mode results.\n- For open-ended searches needing many rounds, use spawn_agent.",
    "parameters": {
      "pattern (required)": "Regular expression to search for. An invalid\npattern is rejected.",
      "path": "Directory to search under. Defaults to the workspace root.",
      "globs": "Array of filename glob filters.",
      "file_type": "ripgrep file-type name (e.g. \"py\", \"rust\").",
      "mode": "\"files\" (default), \"content\", or \"count\".",
      "case_insensitive": "Default false.",
      "line_numbers": "Content mode, default true.",
      "before_context / after_context": "Context lines before/after each\ncontent match. Default 0.",
      "context": "Symmetric context lines; overrides the two above when > 0.",
      "multiline": "Pattern may match across lines. Default false.",
      "head_limit": "Maximum result rows. Default 250; clamps to [1, 1000]. In\ncount mode bounds output rows, not the summed count.",
      "offset": "Files-mode results to skip for pagination. Default 0."
    }
  },
  "Glob": {
    "description": "Fast file-name pattern matching over the session sandbox.\n- Supports glob patterns like \"**/*.js\" or \"src/**/*.ts\".\n- Returns matching file paths sorted by modification time, oldest\n  first.\n- Searches hidden files and does not honor .gitignore, so generated and\n  ignored files are included.\n- Returns at most 100 paths; when more match, the result is flagged\n  truncated — narrow the pattern or scope it with `path`.\n- Use this to find files by name; use Grep to search file contents.\n- For open-ended searches that need many rounds of globbing and\n  grepping, use spawn_agent.",
    "parameters": {
      "pattern (required)": "Glob pattern to match file paths against.",
      "path": "Directory to search under. Defaults to the workspace root."
    }
  },
  "exec_command": {
    "description": "Runs a command in the session sandbox and returns its output, or a\nsession ID for ongoing interaction. Commands run through a non-login\nshell with plain pipes; there is no PTY. A command that finishes within\nthe yield window returns its exit status and bounded stdout/stderr. A\ncommand still running at the yield window is left running and returns a\n`session_id` you poll or drive with write_stdin; such background\nsessions live up to 30 minutes, with at most 16 live at once (a 17th\nreturns task_limit). Output is bounded per stream (about 50 KiB and\n2000 lines each). May pause for user approval before it runs.",
    "parameters": {
      "cmd (required)": "Shell command to execute.",
      "workdir": "Working directory for the command. Defaults to the session\nworking directory.",
      "yield_time_ms": "Wait before yielding output. Defaults to 10000 ms;\neffective range is 250-30000 ms. Set a shorter value only when\nintentionally starting a long-lived or interactive process and you\nwant a session_id promptly.",
      "max_output_tokens": "Output budget. Omit for the default per-stream\nbudget (~50 KiB); a lower value shrinks returned output (about 4\nbytes per token) but cannot raise it above the default."
    }
  },
  "write_stdin": {
    "description": "Writes characters to a running sandbox session and returns its recent\noutput. Pass the `session_id` returned by exec_command. With non-empty\n`chars`, the bytes are written to the process stdin and recent output is\nreturned; with empty or omitted `chars`, the call polls for output\nwithout writing. Output is bounded per stream. An unknown or\nalready-ended session returns task_not_found; `chars` over 32 KiB is\nrejected as invalid_input. May pause for user approval before it runs.",
    "parameters": {
      "session_id (required)": "Identifier of the running sandbox session, as\nreturned by exec_command.",
      "chars": "Bytes to write to the session's stdin. Defaults to empty,\nwhich polls without writing. Maximum 32 KiB.",
      "yield_time_ms": "Wait before yielding output. Non-empty writes default\nto 250 ms and cap at 30000 ms. Empty polls are paced by the platform.",
      "max_output_tokens": "Output budget. Omit for the default per-stream\nbudget (~50 KiB); a lower value shrinks returned output."
    }
  },
  "view_image": {
    "description": "View a local image file from the sandbox filesystem when visual\ninspection is needed. Use this for images already available on disk.\nSupported formats are PNG, JPEG, GIF, and WebP, detected by file\ncontent rather than extension, up to 5 MiB. A missing or unreadable\npath returns not_found; an empty file or non-image returns\nunsupported_format; a file over 5 MiB returns too_large.",
    "parameters": {
      "path (required)": "Filesystem path to an image file, resolved against\nthe session working directory."
    }
  },
  "apply_patch": {
    "description": "The `apply_patch` tool edits files in the session sandbox. This is a\nraw-text tool: pass the patch as a plain string and do NOT wrap it in\nJSON. A patch is a `*** Begin Patch` / `*** End Patch` envelope\ncontaining one or more `*** Add File:` / `*** Update File:` /\n`*** Delete File:` sections; relative paths resolve against the\nworkspace root. May pause for user approval before it runs."
  },
  "web": {
    "description": "Search the public web, fetch a page, or find a pattern inside a page\nyou already fetched. Pass any of three parallel batches in one call:\n`search_query` runs web searches; `open` fetches a URL or re-opens a\nprior result; `find` regex-scans an already-opened document. Search\nreturns ranked result stubs with a short `ref_id` each — open a\n`ref_id` or a URL to read the actual content. Opened content comes back\nas a line-numbered window; large pages paginate via `next_lineno`. Use\nthis only for live internet content; for files in your sandbox use Read\nand Grep. `ref_id`s are temporary and per-session; a stale one returns\n\"invalid or expired ref — re-open by URL.\" Only public http/https hosts\nare reachable; internal, private, and loopback addresses are refused.\nMay pause for user approval before it runs.",
    "parameters": {
      "search_query": "Array of { q (required), domains (≤4) }. Returns stubs\nonly — open a ref or URL to read content.",
      "open": "Array of { url | ref_id (exactly one), lineno }. `lineno`\n(default 1) is the 1-based start line of the returned window; use the\nresponse's `next_lineno` to continue a truncated page.",
      "find": "Array of { ref_id (required), pattern (required) }. `pattern`\nis a regex matched line-by-line against the already-opened document;\nreturns up to 250 matching lines."
    }
  },
  "memory": {
    "description": "Create, replace, delete, or rename a file in your durable memory.\nMemory persists across sessions and is separate from your sandbox\nworking files — write here only what should outlive this session.\nEvery mutation is applied exactly once. `create` writes a new file\n(fails if the path, or any parent/child of it, already exists).\n`replace` does a scoped find-and-replace inside an existing file.\n`delete` and `rename` require you to prove you hold the current content\nvia `expected_text`. Content is capped at 102400 bytes per file. On a\nconflict or stale-precondition failure the result asks you to re-read\nthe current file before retrying — do that rather than re-sending the\nsame call. Paths are relative (no leading slash), at most 1023 bytes,\nwith no `.`/`..` segments. May pause for user approval before it runs.",
    "parameters": {
      "action (required)": "One of create, replace, delete, rename.",
      "path (required)": "Relative path (no leading /, no ./.., ≤1023 bytes).",
      "content": "Required for create. Full file body, ≤102400 bytes.",
      "old_text / new_text": "Required together for replace. old_text must\noccur in the current file; more than one occurrence fails unless\nreplace_all is true.",
      "replace_all": "For replace, default false. True replaces every\noccurrence; false requires a unique match.",
      "expected_text": "Required for delete and rename. Must equal the file's\nentire current content.",
      "new_path": "Required for rename. Same path rules; fails with\npath_exists on collision."
    }
  },
  "spawn_agent": {
    "description": "Create a new child agent and hand it an opening prompt. The child is a\ndurable thread that runs independently of you; you address it later by\nthe `task_name` you assign here. Use this to delegate a self-contained\nunit of work you want to run concurrently or in isolation. The\n`task_name` must be unique among your children — reusing one fails with\n\"already exists.\" By default the child inherits a snapshot of your\nconversation so far (`fork_turns: all`); pass `none` for a clean\ncontext or a positive integer to seed only the last N turns. The call\nreturns the child's `task_name` and `session_thread_id` with `status:\ndelivered` after its opening prompt is durably admitted. The prompt is\ndelivered exactly once, and each completed child turn sends its final\nanswer back as a parent-conversation message. Follow up with\nsend_message and wait_agent. May pause for user approval before it runs.",
    "parameters": {
      "task_name (required)": "Your handle for this child; unique among your\ncurrent children. All later calls reference this name.",
      "prompt (required)": "The opening instruction delivered to the child.",
      "agent_type": "One of general (default), research, worker.",
      "fork_turns": "all (default) seeds the child with your full prior\nconversation; none starts clean; a positive-integer string (e.g.\n\"3\") seeds that many most-recent turns."
    }
  },
  "send_message": {
    "description": "Send a message to a child agent you already spawned, addressed by its\n`task_name`. Use this to give an existing child follow-up work, answers,\nor new instructions — not to create one (use spawn_agent). The named\nchild must exist under you, or the call fails with \"no sub-agent named\n…\"; it must be in a state that can receive input, otherwise it fails as\n\"not receivable in status …\". Delivery is exactly once. The call\nreturns the child's `session_thread_id` with `status: delivered` after\nthe message is durably admitted. It does not wait for the child to\nreply — use wait_agent for that. May pause for user approval before it\nruns.",
    "parameters": {
      "task_name (required)": "The name you gave the child at spawn time.",
      "message (required)": "The message text delivered to that child."
    }
  },
  "wait_agent": {
    "description": "Wait for a child agent, addressed by its `task_name`, to settle its\ncurrent activity. If the child is idle, this returns immediately with\nits current status. If the child is actively working, it returns as\nsoon as the current turn completes and includes the final message.\nThe returned completion remains durably admitted for the parent's next\nlegal turn; repeated reads reuse the same delivery identity. It returns\non the FIRST such signal only — call again to observe further progress.\nPass `timeout_ms` to bound the wait; on expiry the result carries\n`timed_out: true` and the child's status at that moment, not an error.\nThe named child must exist under you. Waiting observes; it does not send\nwork — use send_message for that. May pause for user approval before it\nruns.",
    "parameters": {
      "task_name (required)": "The name of an existing child to wait on.",
      "timeout_ms": "Maximum wait in milliseconds (non-negative). Omit to wait\nuntil the current activity settles."
    }
  },
  "interrupt_agent": {
    "description": "Stops the in-progress work of a running child agent you started,\nwithout closing it. The child keeps its conversation and its place, so\nyou can send it a new message afterward and it continues. Use this to\nreclaim a child that is stuck, looping, or working on something you no\nlonger need. The result reports `interrupted: true` when a running turn\nwas actually stopped, and `interrupted: false` when the child was\nalready idle — both are successful outcomes. Fails when no child with\nthe given name exists under the current thread. May pause for user\napproval before it runs.",
    "parameters": {
      "task_name (required)": "The name you gave the child when you spawned\nit."
    }
  },
  "close_agent": {
    "description": "Releases a child agent and its complete descendant subtree from active\nruntime. Non-terminal durable records are marked `closed_for_runtime`;\nfailed and terminated records keep their terminal outcome. Only after\nthose outcomes are durably recorded is live state torn down, cancelling\nany turns still running in the subtree. The result reports the named\nchild's actual resulting status. A child closed for runtime can be\nreopened later with resume_agent; a terminal child cannot. Fails when\nno child with the given name exists, when the durable close cannot be\nrecorded, or when live-state teardown is refused (retryable). May pause\nfor user approval before it runs.",
    "parameters": {
      "task_name (required)": "The name you gave the child when you spawned\nit."
    }
  },
  "resume_agent": {
    "description": "Reopens a child that is `closed_for_runtime` and reloads its full\ncontext without sending new work. The reopened child settles at `idle`,\nready for the next message. Calling this for an already active child\nensures its hot context is resident and reports its current status.\nFailed and terminated children remain terminal and are not reloaded.\nFails when no child with the given name exists or when a non-terminal\nchild's context cannot be restored. May pause for user approval before\nit runs.",
    "parameters": {
      "task_name (required)": "The name of the closed child to reopen."
    }
  },
  "list_agents": {
    "description": "Lists the child agents that exist under the current thread — running,\nidle, or closed. Returns one entry per child with its task name, a\nstable identifier, its current status, and its agent type. Use it to\nsee which children you have spawned, recover a name, or check whether a\nchild is still active before acting on it. An empty list is a normal\nresult. May pause for user approval before it runs.",
    "parameters": {}
  }
} as const;

/** Builtin name whose provider-facing copy is present in the complete copy table. */
export type DocumentedBuiltinToolName = keyof typeof BuiltinToolCopy;

/** Returns the approved provider-facing description for one builtin tool. */
export function builtinToolDescription(name: DocumentedBuiltinToolName): string {
  return BuiltinToolCopy[name].description;
}

/** Resolves a schema field to its approved provider-facing parameter description. */
export function builtinToolParameterDescription(name: DocumentedBuiltinToolName, parameter: string): string {
  const copy = BuiltinToolCopy[name];
  if (!("parameters" in copy)) {
    throw new Error(`documented parameter ${name}.${parameter} is missing`);
  }
  for (const [label, description] of Object.entries(copy.parameters)) {
    const names = label.replace(/ \(required\)$/, "").split(" / ");
    if (names.includes(parameter) || label === parameter) {
      return description;
    }
  }
  throw new Error(`documented parameter ${name}.${parameter} is missing`);
}
