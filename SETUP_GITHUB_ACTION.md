# Setup GitHub Action for test_simple_game

Due to GitHub security restrictions, GitHub Apps cannot create workflow files directly. To set up the GitHub Action that runs `test_simple_game` and posts results to PRs, follow these steps:

## Step 1: Create the workflow directory
```bash
mkdir -p .github/workflows
```

## Step 2: Move the workflow file
```bash
mv test-simple-game-workflow.yml .github/workflows/test-simple-game.yml
```

## Step 3: Commit and push
```bash
git add .github/workflows/test-simple-game.yml
git commit -m "Add GitHub Action for test_simple_game"
git push
```

## What the GitHub Action does:

1. **Triggers on PR events** (opened, synchronize, reopened)
2. **Sets up required services**:
   - PostgreSQL database (port 5432)
   - Redis server (port 6379)
3. **Runs the `test_simple_game` test** with proper environment variables
4. **Posts detailed results to the PR** including:
   - Pass/fail status with visual indicators (✅/❌)
   - Test duration
   - Exit code
   - Replay file location (if generated)
   - Full test output in a collapsible section
5. **Uploads test artifacts** for debugging

## Expected PR Comment Format:

```
## 🎮 Simple Game Test Results

**Status**: ✅ **PASSED**
**Duration**: 2.3s
**Exit Code**: 0
**Replay File**: `/path/to/replay.file`

<details>
<summary>📋 Test Output</summary>

```
[Full test output here]
```

</details>

---
*This test simulates a simple 2-player Snake game where one snake turns to avoid walls while the other continues straight into a wall.*
```

The workflow is now ready to be moved to the proper location and committed manually.