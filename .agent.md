# Personal Development Guidelines for Claude Code

This document contains my personal development guidelines and preferences for working with Claude Code. These rules override default behavior and ensure consistent practices across all projects.

---

## Core Philosophy

### Best Simple System for Now (BSSN)

Build the **simplest** system that meets the needs **right now**, written to an **appropriate standard**. Avoid both over-engineering and corner-cutting.

#### Core Principles

1. **Design "for Now"** - Focus on what is actually needed RIGHT NOW, not anticipated future needs
2. **Keep it Simple** - No speculative interfaces, abstractions, or generic functionality where specific code is clearer
3. **Write it Best** - Use appropriate quality standards for the context; don't cut corners on core functionality

#### Red Flags to Avoid
- "We might need this later"
- "Let's make this configurable" 
- "What if we have 10,000 users?" (when you have 12)
- Interfaces with single implementations
- Design patterns applied without clear current benefit

---

## Tool Preferences

### Tool Hierarchy
When multiple tools can accomplish the same task, use this order:

1. **Sequential Thinking MCP** - Complex problems requiring step-by-step analysis
2. **Context7 MCP** - Library documentation lookup; always resolve library ID first
3. **Filesystem MCP** - Enhanced file operations over built-in tools
4. **Task Tool** - Complex searches requiring multiple iterations
5. **Built-in Tools** - Simple, direct operations

### Tool Usage Guidelines
- Use Sequential Thinking MCP for problems requiring 3+ steps or unfamiliar domains  
- Use concurrent tool calls when gathering related information
- Use Read tool before making assumptions about file contents
- Prefer Task tool for keyword-based searches across codebases

---

## Task Management

### TodoWrite Usage
**Use for:** Multi-step workflows (3+ actions), complex tasks requiring tracking, multiple user requirements

**Behavior:**
- Mark tasks `in_progress` immediately when starting
- Only ONE task `in_progress` at a time
- Mark `completed` immediately upon finishing
- Break complex tasks into specific, actionable items

---

## Communication & Reasoning

### Response Style
- **Be Direct**: Accurate information, acknowledge limitations, correct mistakes promptly
- **Be Concise**: Clear language, avoid repetition, skip filler phrases, get to the point
- **Be Collaborative**: Treat interactions like junior developer code reviews, propose multiple approaches, ask clarifying questions

### Problem-Solving Approach
1. **Explain Approach** - Describe planned approach step-by-step
2. **Use Sequential Thinking** - For complex tasks requiring reasoning
3. **Validate Against BSSN** - Choose simplest approach that solves current problem
4. **Propose Alternatives** - When multiple valid approaches exist

---

## Elixir Development

### BSSN Application to Elixir
- Start with simplest solution for current problem
- Avoid abstractions unless clearly justified by current needs
- Choose most direct implementation when multiple approaches exist
- Reject speculative flexibility or "future-proofing"

### Code Style & Quality Requirements
1. **Style Guide** - Strictly follow [The Elixir Style Guide](https://github.com/christopheradams/elixir_style_guide/blob/master/README.md)
2. **Documentation First** - Always consult official Elixir/Phoenix documentation
3. **Code Validation** - Verify module/function existence before suggesting code
4. **Explicit Error Handling** - Always generate code with explicit error handling by default
5. **Codebase First** - Use Read tool to examine existing patterns before writing new code

### Testing Preferences
- **DataCase Usage**: Begin tests with `use MyApp.DataCase, async: true`
- **Factory Usage**: ALWAYS use factories for test data, NEVER `Repo.insert` directly
- **Factory Organization**: New factories in `test/support/factories/`, not main factory file
- **Scoped Data**: Always test proper data isolation if application has tenant-like scoping
- **Test Execution**: Must run specific test after creation/update to verify correctness

### Tool Usage for Elixir
- Always use Read tool to examine existing patterns before writing new code
- Use Grep/Glob tools to understand current file organization  
- Never assume module names or function signatures - verify with codebase
- Prefer editing existing files over creating new ones

---

## Code Analysis & Decision Making

### Decision Framework
1. **Codebase First** - Examine existing patterns before suggesting solutions
2. **Documentation Verification** - Verify function signatures and module existence
3. **BSSN Application** - Choose simplest approach among valid options
4. **Ambiguity Resolution** - Ask specific questions about current needs rather than building flexible solutions

---

## Project Management

### GitHub Operations
- Always use `gh` CLI instead of web URLs for GitHub tasks
- Use `gh` for issues, pull requests, checks, and releases

### Workflow Preferences  
- Follow MCP tool hierarchy: MCP tools > Task tool > built-in tools
- Use concurrent tool calls when gathering related information
- Use TodoWrite for multi-step tasks requiring progress tracking

---

# Important Reminders
- Do what has been asked; nothing more, nothing less
- NEVER create files unless absolutely necessary for achieving the goal
- ALWAYS prefer editing existing files over creating new ones
- NEVER proactively create documentation files unless explicitly requested