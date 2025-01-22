import requests
import fnmatch
import logging
from math import ceil
from typing import List, Dict, Set
from dependency_parser_lib.language_depfiles import LANGUAGE_DEPENDENCY_FILES

logger = logging.getLogger("smart_search")
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------------------------------------
# 1) Collect all file paths via tree(...) -> blobs(...) -> path
# --------------------------------------------------------------------------------

LIST_PATHS_QUERY = """
query FetchPaths($fullPath: ID!, $ref: String!, $after: String) {
  project(fullPath: $fullPath) {
    repository {
      tree(ref: $ref, recursive: true) {
        blobs(first: 100, after: $after) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            path
          }
        }
      }
    }
  }
}
"""

def fetch_all_paths_graphql(token: str, full_path: str, ref: str) -> List[str]:
    """
    Uses GitLab GraphQL to list all file paths in the repository (up to pagination).
    Returns a list of file paths like ["README.md", "src/main.py", "subdir/requirements.txt", ...].
    """

    base_url = "https://gitlab.com/api/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    all_file_paths = []
    after_cursor = None

    while True:
        variables = {
            "fullPath": full_path,
            "ref": ref,
            "after": after_cursor
        }
        logger.info(f"GraphQL: fetching file paths, after={after_cursor} ...")

        resp = requests.post(
            base_url,
            json={"query": LIST_PATHS_QUERY, "variables": variables},
            headers=headers
        )
        if resp.status_code != 200:
            raise Exception(f"GitLab GraphQL error: {resp.status_code} {resp.text}")

        data = resp.json()
        # Optional debug:
        # logger.debug(f"Paths response: {data}")

        tree_data = data.get("data", {}).get("project", {}).get("repository", {}).get("tree", {})
        if not tree_data:
            logger.warning("No project/repository data returned. Possibly no access or project not found.")
            break

        blobs = tree_data.get("blobs", {})
        nodes = blobs.get("nodes", [])

        for blob in nodes:
            path = blob.get("path")
            if path:
                all_file_paths.append(path)

        page_info = blobs.get("pageInfo", {})
        has_next = page_info.get("hasNextPage")
        end_cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(nodes)} paths in this batch. Total so far: {len(all_file_paths)}")

        if has_next:
            after_cursor = end_cursor
        else:
            logger.info("No more pages. Done collecting all file paths.")
            break

    return all_file_paths

# --------------------------------------------------------------------------------
# 2) Filter paths by fnmatch patterns from LANGUAGE_DEPENDENCY_FILES
# --------------------------------------------------------------------------------

def filter_paths_by_dependency_patterns(all_paths: List[str]) -> List[str]:
    """
    Returns only those paths that match at least one of the patterns in LANGUAGE_DEPENDENCY_FILES.
    """
    # Collect all patterns from LANGUAGE_DEPENDENCY_FILES
    all_patterns = []
    for patterns_dict in LANGUAGE_DEPENDENCY_FILES.values():
        all_patterns.extend(patterns_dict.keys())

    matching = []
    for file_path in all_paths:
        # If at least one pattern matches, we keep the file
        for pat in all_patterns:
            if fnmatch.fnmatch(file_path, pat):
                matching.append(file_path)
                break
    return matching

# --------------------------------------------------------------------------------
# 3) For the filtered paths, fetch content with blobs(paths=[...]) -> rawTextBlob
#    But we can only request up to 100 paths at a time, so we chunk them.
# --------------------------------------------------------------------------------

RAWBLOB_QUERY = """
query FetchRawBlobs($fullPath: ID!, $ref: String!, $paths: [String!]!) {
  project(fullPath: $fullPath) {
    repository {
      blobs(ref: $ref, paths: $paths) {
        nodes {
          path
          # rawTextBlob is only available if the file is not too large/binary
          rawTextBlob
        }
      }
    }
  }
}
"""

def fetch_raw_texts_graphql(token: str, full_path: str, ref: str, paths_batch: List[str]) -> Dict[str, str]:
    """
    Given up to 100 file paths, uses GitLab GraphQL to fetch 'rawTextBlob' for each path.
    Returns a dict: { file_path: rawText or None }.
    """

    base_url = "https://gitlab.com/api/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    variables = {
        "fullPath": full_path,
        "ref": ref,
        "paths": paths_batch
    }

    resp = requests.post(
        base_url,
        json={"query": RAWBLOB_QUERY, "variables": variables},
        headers=headers
    )
    if resp.status_code != 200:
        raise Exception(f"GitLab GraphQL error: {resp.status_code} {resp.text}")

    data = resp.json()
    # logger.debug(f"RawText response for {paths_batch}: {data}")

    repo_data = data.get("data", {}).get("project", {}).get("repository", {})
    blobs = repo_data.get("blobs", {}).get("nodes", [])

    result = {}
    for blob in blobs:
        p = blob.get("path")
        raw_text = blob.get("rawTextBlob")
        result[p] = raw_text

    return result

# --------------------------------------------------------------------------------
# 4) Parse the downloaded text using LANGUAGE_DEPENDENCY_FILES
# --------------------------------------------------------------------------------

def parse_files_for_dependencies(file_contents: Dict[str, str]) -> Dict[str, Set[str]]:
    """
    file_contents: { "requirements.txt": "<file text>", ... }
    We check each path against LANGUAGE_DEPENDENCY_FILES and parse it if matched.
    Return structure: {language: set_of_dependencies}
    """
    user_language_dependencies = {}

    for path, text in file_contents.items():
        if not text:  # Could be None or empty if file is too large/binary
            continue

        # Find matching parser
        for lang, patterns_dict in LANGUAGE_DEPENDENCY_FILES.items():
            for pattern, parser_fn in patterns_dict.items():
                if fnmatch.fnmatch(path, pattern):
                    logger.info(f"File {path} matched pattern {pattern} for language {lang}")
                    try:
                        deps = parser_fn(text)
                        if deps:
                            if lang not in user_language_dependencies:
                                user_language_dependencies[lang] = set()
                            user_language_dependencies[lang].update(deps)
                            logger.info(f"Dependencies found in {path}: {deps}")
                        else:
                            logger.warning(f"No dependencies found in {path}")
                    except Exception as e:
                        logger.error(f"Error parsing dependencies in {path}: {e}")

    return user_language_dependencies

# --------------------------------------------------------------------------------
# Main function combining all steps
# --------------------------------------------------------------------------------

def parse_dependencies_full_graphql(
    project_url: str,
    token: str,
    branch: str = "main"
) -> Dict[str, Set[str]]:
    """
    1) Collect *all* file paths from the GitLab repository via GraphQL (tree).
    2) Filter them by patterns from LANGUAGE_DEPENDENCY_FILES.
    3) Chunk these paths (<= 100 each) and fetch rawTextBlob via GraphQL's blobs(paths=...).
    4) Parse them for dependencies.
    5) Return {language: set_of_dependencies}.
    """
    logger.info("Starting parse_dependencies_full_graphql...")

    # Extract "group/project" from e.g. "https://gitlab.com/group/project"
    full_path = "/".join(project_url.strip("/").split("/")[3:])
    logger.info(f"Working on {full_path} (branch: {branch})")

    # Step 1: Collect all paths
    all_paths = fetch_all_paths_graphql(token, full_path, branch)
    logger.info(f"Total {len(all_paths)} files in repo.")

    if not all_paths:
        return {}

    # Step 2: Filter them by known patterns
    candidate_paths = filter_paths_by_dependency_patterns(all_paths)
    logger.info(f"{len(candidate_paths)} files match known dependency patterns.")

    if not candidate_paths:
        return {}

    # Step 3: Chunk them in batches of up to 100
    user_language_dependencies = {}

    BATCH_SIZE = 100
    num_batches = ceil(len(candidate_paths) / BATCH_SIZE)
    logger.info(f"Fetching rawTextBlob in {num_batches} batch(es).")

    start_index = 0
    for batch_i in range(num_batches):
        batch_paths = candidate_paths[start_index : start_index + BATCH_SIZE]
        start_index += BATCH_SIZE

        logger.info(f"Batch {batch_i+1}/{num_batches}: {len(batch_paths)} files.")
        # Fetch raw text for these files
        path_to_text = fetch_raw_texts_graphql(token, full_path, branch, batch_paths)

        # Step 4: Parse them
        batch_deps = parse_files_for_dependencies(path_to_text)
        # Merge into main dictionary
        for lang, deps in batch_deps.items():
            if lang not in user_language_dependencies:
                user_language_dependencies[lang] = set()
            user_language_dependencies[lang].update(deps)

    logger.info("Finished parse_dependencies_full_graphql.")
    return user_language_dependencies

# --------------------------------------------------------------------------------
# Example usage
# --------------------------------------------------------------------------------

def main():
    """
    Example usage of parse_dependencies_full_graphql,
    fetching & parsing dependencies with pure GraphQL (no REST).
    """
    project_url = "https://gitlab.com/gitlab-org/gitaly"
    token = "glpat-8rre52gVcx-zs5L3qxGN"
    branch = "master"  # or "develop", or "main"

    deps = parse_dependencies_full_graphql(project_url, token, branch)
    print("Dependencies found (pure GraphQL):")
    for lang, dep_list in deps.items():
        print(f"Language: {lang}, Dependencies: {sorted(dep_list)}")

if __name__ == "__main__":
    main()
