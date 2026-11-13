#!/bin/bash

# Release script, in two steps:
#   1. ./tag_version.sh <version> [--dry-run]
#      Updates CHANGELOG.md on a release/v<version> branch and opens a PR.
#   2. ./tag_version.sh <version> --tag [--dry-run]
#      Once the PR is merged, tags the release commit on main and creates the GitHub release.

set -e

DRY_RUN=false
TAG_MODE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --tag)
            TAG_MODE=true
            shift
            ;;
        *)
            VERSION="$1"
            shift
            ;;
    esac
done

if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version> [--tag] [--dry-run]"
    echo "Example: $0 11.0.1             # open the release PR (CHANGELOG update)"
    echo "Example: $0 11.0.1 --tag       # after the PR is merged: tag + GitHub release"
    echo "Example: $0 11.0.1 --dry-run"
    exit 1
fi

RELEASE_BRANCH="release/v$VERSION"

DATE=$(date +%Y-%m-%d)

if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN MODE]"
    echo ""
fi

# Check if gh CLI is installed and configured
if ! command -v gh &> /dev/null; then
    echo "Error: GitHub CLI (gh) is not installed"
    echo "Please install it: https://cli.github.com/"
    exit 1
fi

if ! gh auth status &> /dev/null; then
    echo "Error: GitHub CLI is not authenticated"
    echo "Please run: gh auth login"
    exit 1
fi

# Detect main branch (master or main)
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if git rev-parse --verify master >/dev/null 2>&1; then
    MAIN_BRANCH="master"
elif git rev-parse --verify main >/dev/null 2>&1; then
    MAIN_BRANCH="main"
else
    echo "Error: Could not detect main branch (neither master nor main exists)"
    exit 1
fi

# Check if we're on the main branch
if [ "$CURRENT_BRANCH" != "$MAIN_BRANCH" ]; then
    echo "Error: You must be on the $MAIN_BRANCH branch to create a release"
    echo "Current branch: $CURRENT_BRANCH"
    exit 1
fi

# Check if working copy is clean (skip in dry-run mode)
if [ "$DRY_RUN" = false ] && ! git diff-index --quiet HEAD --; then
    echo "Error: Working copy is not clean. Please commit or stash your changes."
    git status --short
    exit 1
fi

# Check if we're up to date with remote (skip in dry-run mode)
if [ "$DRY_RUN" = false ]; then
    git fetch origin "$MAIN_BRANCH" --quiet
    LOCAL=$(git rev-parse @)
    REMOTE=$(git rev-parse @{u})

    if [ "$LOCAL" != "$REMOTE" ]; then
        echo "Error: Your local $MAIN_BRANCH branch is not up to date with origin/$MAIN_BRANCH"
        echo "Please run: git pull origin $MAIN_BRANCH"
        exit 1
    fi
fi

# Check if tag already exists
if git rev-parse "v$VERSION" >/dev/null 2>&1; then
    echo "Error: Tag v$VERSION already exists"
    exit 1
fi

# Step 2: tag the merged release commit and create the GitHub release
if [ "$TAG_MODE" = true ]; then
    # Find the commit on main that added this version's CHANGELOG entry
    RELEASE_COMMIT=$(git log -1 --format=%H -S "## $VERSION (" -- CHANGELOG.md)
    if [ -z "$RELEASE_COMMIT" ]; then
        echo "Error: No CHANGELOG entry for $VERSION found on $MAIN_BRANCH"
        echo "Has the release PR ($RELEASE_BRANCH) been merged, and is your $MAIN_BRANCH up to date?"
        exit 1
    fi

    # Extract release notes from the CHANGELOG section of this version
    RELEASE_NOTES=$(awk -v header="## $VERSION (" '
        index($0, header) == 1 { in_section = 1; next }
        in_section && /^## / { exit }
        in_section { print }
    ' CHANGELOG.md)

    echo "Release commit: $(git log -1 --format='%h %s' "$RELEASE_COMMIT")"

    if [ "$DRY_RUN" = true ]; then
        echo "Release notes:"
        echo "$RELEASE_NOTES"
        echo ""
        echo "Would run: git tag -a \"v$VERSION\" -m \"Version $VERSION\" $RELEASE_COMMIT"
        echo "Would run: git push origin v$VERSION"
        echo "Would run: gh release create \"v$VERSION\" --title \"v$VERSION\" --notes <release notes>"
        exit 0
    fi

    git tag -a "v$VERSION" -m "Version $VERSION" "$RELEASE_COMMIT"
    echo "✓ Created tag v$VERSION"

    git push origin "v$VERSION"
    echo "✓ Pushed tag v$VERSION to origin"

    echo "$RELEASE_NOTES" | gh release create "v$VERSION" \
        --title "v$VERSION" \
        --notes-file -
    echo "✓ Created GitHub release v$VERSION"
    exit 0
fi

# Step 1: check the release branch doesn't already exist
if git rev-parse --verify "$RELEASE_BRANCH" >/dev/null 2>&1; then
    echo "Error: Local branch $RELEASE_BRANCH already exists"
    exit 1
fi
if [ -n "$(git ls-remote --heads origin "$RELEASE_BRANCH" 2>/dev/null)" ]; then
    echo "Error: Branch $RELEASE_BRANCH already exists on origin"
    exit 1
fi

# Get repository URL for PR links
GIT_REMOTE=$(git remote get-url origin 2>/dev/null || echo "")
if [[ "$GIT_REMOTE" =~ github.com[:/]([^/]+)/([^/.]+) ]]; then
    REPO_OWNER="${BASH_REMATCH[1]}"
    REPO_NAME="${BASH_REMATCH[2]}"
    REPO_URL="https://github.com/$REPO_OWNER/$REPO_NAME"
else
    REPO_URL=""
fi

# Get the last tag
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")

if [ -z "$LAST_TAG" ]; then
    echo "No previous tag found, getting all commits"
    COMMIT_RANGE=""
else
    echo "Getting commits since $LAST_TAG"
    COMMIT_RANGE="$LAST_TAG..HEAD"
fi

# Get commit hashes
if [ -z "$COMMIT_RANGE" ]; then
    COMMIT_HASHES=$(git log --pretty=format:"%H" --no-merges)
else
    COMMIT_HASHES=$(git log "$COMMIT_RANGE" --pretty=format:"%H" --no-merges)
fi

# Check if there are commits
if [ -z "$COMMIT_HASHES" ]; then
    echo "No new commits since last tag"
    exit 1
fi

# Process commits: separate breaking changes and sort
# Use a delimiter to separate commits (a string that won't appear in commit messages)
COMMIT_DELIMITER="<<<COMMIT_SEPARATOR>>>"
BREAKING_CHANGES_RAW=""
REGULAR_COMMITS_RAW=""

while IFS= read -r hash; do
    # Get subject and body
    subject=$(git log -1 --pretty=format:"%s" "$hash" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    body=$(git log -1 --pretty=format:"%b" "$hash")

    # Convert PR numbers to links if we have a repo URL
    if [ -n "$REPO_URL" ]; then
        subject=$(echo "$subject" | sed -E "s|#([0-9]+)|[#\1]($REPO_URL/pull/\1)|g")
        body=$(echo "$body" | sed -E "s|#([0-9]+)|[#\1]($REPO_URL/pull/\1)|g")
    fi

    # Check if it's a breaking change (contains ! before :)
    if [[ "$subject" =~ ^[a-z]+(\([^\)]+\))?\!: ]]; then
        # Make subject bold in markdown
        commit_entry="- **$subject**"

        # Add body if it exists, indented with two spaces for markdown
        if [ -n "$body" ]; then
            # Indent body lines with two spaces and preserve newlines
            indented_body=$(echo "$body" | sed 's/^/  /')
            commit_entry="${commit_entry}"$'\n'"${indented_body}"
        fi

        BREAKING_CHANGES_RAW="${BREAKING_CHANGES_RAW}${commit_entry}${COMMIT_DELIMITER}"$'\n'
    else
        REGULAR_COMMITS_RAW="${REGULAR_COMMITS_RAW}- ${subject}${COMMIT_DELIMITER}"$'\n'
    fi
done <<< "$COMMIT_HASHES"

# Sort breaking changes (sort by first line only, keep blocks together)
BREAKING_CHANGES=""
if [ -n "$BREAKING_CHANGES_RAW" ]; then
    # Split by delimiter, sort by first line, then rejoin
    BREAKING_CHANGES=$(echo "$BREAKING_CHANGES_RAW" | \
        awk -v delim="$COMMIT_DELIMITER" '
        BEGIN { RS=delim"\n"; ORS="" }
        NF {
            # Store the first line as sort key and full content
            key = $0
            sub(/\n.*/, "", key)
            # Store in array
            keys[NR] = key
            commits[NR] = $0
        }
        END {
            # Simple bubble sort
            n = NR
            for (i = 1; i <= n; i++) {
                for (j = i + 1; j <= n; j++) {
                    if (keys[i] > keys[j]) {
                        tmp = keys[i]
                        keys[i] = keys[j]
                        keys[j] = tmp
                        tmp = commits[i]
                        commits[i] = commits[j]
                        commits[j] = tmp
                    }
                }
            }
            for (i = 1; i <= n; i++) {
                print commits[i] "\n"
            }
        }
    ')
fi

# Sort regular commits
REGULAR_COMMITS=""
if [ -n "$REGULAR_COMMITS_RAW" ]; then
    REGULAR_COMMITS=$(echo "$REGULAR_COMMITS_RAW" | sed "s/${COMMIT_DELIMITER}//g" | sort)$'\n'
fi

# Combine: breaking changes first, then regular commits
SORTED_COMMITS=""
if [ -n "$BREAKING_CHANGES" ]; then
    SORTED_COMMITS="$BREAKING_CHANGES"
fi
if [ -n "$REGULAR_COMMITS" ]; then
    SORTED_COMMITS="${SORTED_COMMITS}${REGULAR_COMMITS}"
fi

# Prepare the new changelog entry
NEW_ENTRY="## $VERSION ($DATE)
$SORTED_COMMITS
"

# Prepare release notes for GitHub
RELEASE_NOTES="$SORTED_COMMITS"

# Update CHANGELOG.md
if [ "$DRY_RUN" = true ]; then
    echo "Would update CHANGELOG.md with:"
    echo "$NEW_ENTRY"
    echo "Would run: git checkout -b $RELEASE_BRANCH"
    echo "Would run: git add CHANGELOG.md"
    echo "Would run: git commit -m \"chore: release $VERSION (CHANGELOG)\""
    echo "Would run: git push -u origin $RELEASE_BRANCH"
    echo "Would run: gh pr create --base $MAIN_BRANCH --head $RELEASE_BRANCH --title \"chore: release $VERSION (CHANGELOG)\" --body <release notes>"
    echo "Would run: git checkout $MAIN_BRANCH"
    exit 0
fi

if [ -f "CHANGELOG.md" ]; then
    # Keep existing entries (skip the first line "# Changelog")
    EXISTING_ENTRIES=$(tail -n +2 CHANGELOG.md)
    echo "# Changelog

$NEW_ENTRY$EXISTING_ENTRIES" > CHANGELOG.md
else
    # Create new CHANGELOG.md
    echo "# Changelog

$NEW_ENTRY" > CHANGELOG.md
fi

echo "CHANGELOG.md updated with commits from $LAST_TAG to HEAD"

# Commit CHANGELOG update on a release branch
git checkout -b "$RELEASE_BRANCH"
git add CHANGELOG.md
git commit -m "chore: release $VERSION (CHANGELOG)"

echo "✓ Committed CHANGELOG.md on $RELEASE_BRANCH"

# Push the release branch
git push -u origin "$RELEASE_BRANCH"

echo "✓ Pushed $RELEASE_BRANCH to origin"

# Open the release PR
PR_URL=$(echo "$RELEASE_NOTES" | gh pr create \
    --base "$MAIN_BRANCH" \
    --head "$RELEASE_BRANCH" \
    --title "chore: release $VERSION (CHANGELOG)" \
    --body-file -)

echo "✓ Opened release PR: $PR_URL"

git checkout "$MAIN_BRANCH"

echo ""
echo "Once the PR is merged, run:"
echo "  git pull origin $MAIN_BRANCH && $0 $VERSION --tag"
