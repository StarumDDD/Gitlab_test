import requests
import fnmatch
import logging
from urllib.parse import quote

from dependency_parser_lib.language_depfiles import LANGUAGE_DEPENDENCY_FILES

logger = logging.getLogger("smart_search")
logging.basicConfig(level=logging.INFO)


class QueryRepositoryFileListPaginated:
    """
    GraphQL query to fetch file paths (blobs) with pagination (no string.Template).
    We directly store the query string and pass variables to GitLab.
    """
    def __init__(self):
        self.text = """
        query FetchBlobs($fullPath: ID!, $branch: String!, $after: String) {
          project(fullPath: $fullPath) {
            repository {
              tree(ref: $branch, recursive: true) {
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


class GitLabGraphQLAPI:
    """Class for interacting with GitLab GraphQL API."""
    BASE_URL = "https://gitlab.com/api/graphql"

    def __init__(self, token: str = None):
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def execute_query(self, query: QueryRepositoryFileListPaginated, variables: dict):
        """Executes the GraphQL query with variables and returns the JSON response."""
        logger.info("Executing paginated GraphQL query...")
        payload = {
            "query": query.text,
            "variables": variables
        }
        response = requests.post(self.BASE_URL, json=payload, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"GitLab GraphQL API error: {response.status_code} {response.text}")
        return response.json()


def list_repo_files_via_graphql_paginated(full_path: str, branch: str, token: str):
    """
    Returns a list of file paths (blobs) in a GitLab repository by using a paginated GraphQL query.
    """
    logger.info(f"Listing repository files for branch '{branch}' via GraphQL with pagination...")

    api = GitLabGraphQLAPI(token=token)
    query = QueryRepositoryFileListPaginated()

    all_file_paths = []
    after_cursor = None

    while True:
        variables = {
            "fullPath": full_path,
            "branch": branch,
            "after": after_cursor
        }
        response = api.execute_query(query, variables=variables)
        logger.debug(f"GraphQL raw response: {response}")

        data = response.get("data", {}).get("project", {}).get("repository", {}).get("tree", {})
        blobs = data.get("blobs", {})
        nodes = blobs.get("nodes", [])

        for blob in nodes:
            path = blob.get("path")
            if path:
                all_file_paths.append(path)

        page_info = blobs.get("pageInfo", {})
        has_next = page_info.get("hasNextPage")
        end_cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(nodes)} files in this batch, total so far: {len(all_file_paths)}")

        if has_next:
            after_cursor = end_cursor
            logger.info(f"Continuing to next page with cursor '{after_cursor}'")
        else:
            logger.info("No more pages. Finished fetching all file paths.")
            break

    return all_file_paths


def get_raw_file_content(project_path: str, file_path: str, branch: str, token: str) -> str:
    """
    Uses the GitLab REST API to get the raw content of a file in a repository.
    """
    encoded_project = quote(project_path, safe="")
    encoded_file = quote(file_path, safe="")

    url = f"https://gitlab.com/api/v4/projects/{encoded_project}/repository/files/{encoded_file}/raw"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"ref": branch}

    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.text
    else:
        logger.warning(f"Failed to fetch raw content for {file_path}: {resp.status_code} {resp.text}")
        return ""


def parse_gitlab_dependencies(project_url: str, token: str, branch: str = "main"):
    """
    1) Extracts the project path 'group/project' from the project_url
    2) Lists file paths (paginated) via GraphQL
    3) Filters file paths by fnmatch patterns from LANGUAGE_DEPENDENCY_FILES
    4) For each matching file path, fetches raw file content (REST)
    5) Parses dependencies using LANGUAGE_DEPENDENCY_FILES
    """
    logger.info("Starting parse_gitlab_dependencies...")
    project_path = "/".join(project_url.strip("/").split("/")[3:])
    logger.info(f"Project path: {project_path}, branch: {branch}")

    # Step 2: Paginated file listing
    all_file_paths = list_repo_files_via_graphql_paginated(project_path, branch, token)
    if not all_file_paths:
        logger.warning("No files found in the repository (or you have no access).")
        return {}

    # Step 3: Create a set of all patterns (fnmatch) that we care about
    all_patterns = []
    for patterns_dict in LANGUAGE_DEPENDENCY_FILES.values():
        for pattern in patterns_dict.keys():
            all_patterns.append(pattern)

    # Filter file paths by any matching pattern
    matching_file_paths = []
    for file_path in all_file_paths:
        # We only keep the file if it matches at least one pattern
        for pattern in all_patterns:
            if fnmatch.fnmatch(file_path, pattern):
                matching_file_paths.append(file_path)
                break

    logger.info(f"{len(matching_file_paths)} files match known dependency patterns out of {len(all_file_paths)} total.")

    user_language_dependencies = {}

    # Step 4 and 5: Fetch raw content and parse
    for file_path in matching_file_paths:
        content = get_raw_file_content(project_path, file_path, branch, token)
        if not content:
            continue

        # Match again by language to pick the right parser
        for lang, patterns in LANGUAGE_DEPENDENCY_FILES.items():
            for pattern, parser in patterns.items():
                if fnmatch.fnmatch(file_path, pattern):
                    logger.info(f"File {file_path} matched pattern '{pattern}' for language {lang}")
                    try:
                        dependencies = parser(content)
                        if dependencies:
                            logger.info(f"Dependencies found in {file_path}: {dependencies}")
                        else:
                            logger.warning(f"No dependencies found in {file_path} using parser {parser.__name__}")

                        if lang not in user_language_dependencies:
                            user_language_dependencies[lang] = set()
                        user_language_dependencies[lang].update(dependencies)
                    except Exception as e:
                        logger.error(f"Error parsing dependencies in {file_path}: {e}")

    logger.info("Finished parsing dependencies.")
    return user_language_dependencies


def smart_search_with_graphql(project_url: str, token: str, branch: str = "main"):
    """
    Wrapper function to parse dependencies in a GitLab repository with pagination for all files,
    filtering them by known dependency-file patterns before downloading.
    """
    logger.info("Starting smart_search_with_graphql...")
    try:
        dependencies = parse_gitlab_dependencies(project_url, token, branch)
        return dependencies
    except Exception as e:
        logger.error(f"Error during smart_search_with_graphql: {e}")
        return {}


def invoke_smart_search():
    """
    Test function to demonstrate usage of 'smart_search_with_graphql'.
    """
    project_url = "https://gitlab.com/cerfacs/batman"
    token = "glpat-8rre52gVcx-zs5L3qxGN"
    branch = "develop"  # or whichever branch you actually need
    deps = smart_search_with_graphql(project_url, token, branch)
    print("Dependencies (for main integration):", deps)


if __name__ == "__main__":
    invoke_smart_search()
