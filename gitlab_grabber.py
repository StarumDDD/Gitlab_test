import requests
import fnmatch
import logging
from math import ceil
from typing import List, Dict, Set, Optional

from dependency_parser_lib.language_depfiles import LANGUAGE_DEPENDENCY_FILES

logger = logging.getLogger("smart_search")
logging.basicConfig(level=logging.INFO)

LANGS_AND_PATHS_QUERY = """
query FetchLangsAndPaths($fullPath: ID!, $ref: String!, $after: String) {
  project(fullPath: $fullPath) {
    languages {
      name
      share
    }
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

def fetch_languages_and_paths(
    token: str,
    full_path: str,
    ref: str,
    after_cursor: Optional[str] = None
) -> (List[Dict], List[str], Dict):
    url = "https://gitlab.com/api/graphql"
    headers = {
        "PRIVATE-TOKEN": token,
        "Content-Type": "application/json"
    }
    variables = {
        "fullPath": full_path,
        "ref": ref,
        "after": after_cursor
    }
    payload = {
        "query": LANGS_AND_PATHS_QUERY,
        "variables": variables
    }

    logger.debug(f"Calling fetch_languages_and_paths with after={after_cursor}")
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        raise Exception(f"GitLab GraphQL error: {response.status_code} {response.text}")

    data = response.json()
    project = data.get("data", {}).get("project")
    if not project:
        logger.warning("No project object returned. Possibly no access.")
        return [], [], {}

    langs_data = project.get("languages", [])
    blobs = project.get("repository", {}).get("tree", {}).get("blobs", {})
    nodes = blobs.get("nodes", [])
    file_paths = [n["path"] for n in nodes if "path" in n]
    page_info = blobs.get("pageInfo", {})
    return langs_data, file_paths, page_info

def fetch_all_paths_with_langs(token: str, full_path: str, ref: str) -> (List[Dict], List[str]):
    all_paths: List[str] = []
    final_langs: List[Dict] = []
    after = None

    while True:
        langs, paths, page_info = fetch_languages_and_paths(token, full_path, ref, after)
        # On the first loop, store the languages
        if not all_paths:
            final_langs = langs

        all_paths.extend(paths)
        has_next = page_info.get("hasNextPage")
        end_cursor = page_info.get("endCursor")

        if has_next:
            logger.info(f"Pagination: after={end_cursor}, totalFilesSoFar={len(all_paths)}")
            after = end_cursor
        else:
            logger.info(f"No more pages. Collected total {len(all_paths)} files.")
            break

    return final_langs, all_paths

def pick_primary_language(langs: List[Dict]) -> Optional[str]:
    if not langs:
        return None
    # Sort descending by share
    sorted_langs = sorted(langs, key=lambda x: x.get("share", 0), reverse=True)
    return sorted_langs[0].get("name")

RAWBLOB_QUERY = """
query FetchRawBlobs($fullPath: ID!, $ref: String!, $paths: [String!]!) {
  project(fullPath: $fullPath) {
    repository {
      blobs(ref: $ref, paths: $paths) {
        nodes {
          path
          rawTextBlob
        }
      }
    }
  }
}
"""

def fetch_raw_texts(token: str, full_path: str, ref: str, file_paths: List[str]) -> Dict[str, str]:
    url = "https://gitlab.com/api/graphql"
    headers = {
        "PRIVATE-TOKEN": token,
        "Content-Type": "application/json"
    }
    variables = {
        "fullPath": full_path,
        "ref": ref,
        "paths": file_paths
    }
    payload = {
        "query": RAWBLOB_QUERY,
        "variables": variables
    }

    logger.debug(f"fetch_raw_texts: requesting {len(file_paths)} paths.")
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        raise Exception(f"GitLab GraphQL error: {response.status_code} {response.text}")

    data = response.json()
    nodes = data.get("data", {}).get("project", {}).get("repository", {}).get("blobs", {}).get("nodes", [])
    result = {}
    for blob in nodes:
        p = blob.get("path")
        txt = blob.get("rawTextBlob")
        result[p] = txt
    return result

def parse_primary_lang_files(
    token: str,
    full_path: str,
    ref: str,
    all_file_paths: List[str],
    primary_lang: str
) -> Dict[str, Set[str]]:
    if primary_lang not in LANGUAGE_DEPENDENCY_FILES:
        logger.warning(f"Primary language '{primary_lang}' is not in LANGUAGE_DEPENDENCY_FILES.")
        return {}

    patterns = list(LANGUAGE_DEPENDENCY_FILES[primary_lang].keys())
    matched = []
    for path in all_file_paths:
        for pat in patterns:
            if fnmatch.fnmatch(path, pat):
                matched.append(path)
                break

    logger.info(f"{len(matched)} files match patterns for {primary_lang}")
    if not matched:
        return {}

    user_lang_deps: Dict[str, Set[str]] = {}
    BATCH_SIZE = 100
    total_batches = ceil(len(matched) / BATCH_SIZE)

    patterns_dict = LANGUAGE_DEPENDENCY_FILES[primary_lang]
    for i in range(total_batches):
        batch_paths = matched[i * BATCH_SIZE : (i+1) * BATCH_SIZE]
        path_to_text = fetch_raw_texts(token, full_path, ref, batch_paths)

        for file_path, content in path_to_text.items():
            if not content:
                continue

            # find which pattern specifically matched
            for pat, parser_fn in patterns_dict.items():
                if fnmatch.fnmatch(file_path, pat):
                    try:
                        deps = parser_fn(content)
                        if deps:
                            if primary_lang not in user_lang_deps:
                                user_lang_deps[primary_lang] = set()
                            user_lang_deps[primary_lang].update(deps)
                            logger.info(f"Found deps in {file_path}: {deps}")
                        else:
                            logger.warning(f"No dependencies found in {file_path}")
                    except Exception as e:
                        logger.error(f"Error parsing {file_path}: {e}")

    return user_lang_deps

def parse_gitlab_repo_primary_lang(
    project_url: str,
    token: str,
    ref: str = "main"
) -> Dict[str, Set[str]]:
    logger.info("Starting parse_gitlab_repo_primary_lang...")

    full_path = "/".join(project_url.strip("/").split("/")[3:])
    logger.info(f"Full path: {full_path}, ref: {ref}")

    langs, all_paths = fetch_all_paths_with_langs(token, full_path, ref)
    if not all_paths:
        logger.warning("No file paths found or no access.")
        return {}

    primary = pick_primary_language(langs)
    if not primary:
        logger.warning("No primary language detected.")
        return {}

    logger.info(f"Primary language: {primary}")

    deps = parse_primary_lang_files(token, full_path, ref, all_paths, primary)
    logger.info("parse_gitlab_repo_primary_lang finished.")
    return deps

def main():
    project_url = "https://gitlab.com/gitlab-org/gitlab-runner"
    token = "glpat-KvAmGABXbknHMqNjUSud"
    branch = "main"

    result = parse_gitlab_repo_primary_lang(project_url, token, branch)
    print("Final dependencies:", result)

if __name__ == "__main__":
    main()
