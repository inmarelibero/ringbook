export const REPO = "https://github.com/inmarelibero/ringbook";

/** The branch the source links point at. */
export const SOURCE_REF = "feature/first-milestone";

/** Links to a file or folder in the repository. */
export function source(path: string): string {
  const kind = path.endsWith("/") ? "tree" : "blob";
  return `${REPO}/${kind}/${SOURCE_REF}/${path.replace(/\/$/, "")}`;
}
