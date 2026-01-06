# myKaarma Kafka Connect

## Build assets and release (PR comment trigger)
Builds a Maven JAR for a PR, packages it into `assets/<project-name>/`, commits assets and checksums back to the PR branch, then tags and creates a GitHub release with the ZIP attached.

---

### How to run

On an open pull request, add a comment in the following format:
`/release <project-name>`

### Example: 
`/release mk-chargeover-source-connector` 

### What this does

This workflow will:

1. Check out the PR head branch  
2. Read the Maven version from `<project-name>/pom.xml`  
3. Build and package the JAR  
4. Recreate `assets/<project-name>/` and produce:
   - `<project-name>.zip`
   - `checksums/<project-name>.sha512`
5. Commit and push assets and checksums to the PR branch  
6. Tag `<project-name>-<version>` and create a GitHub release with the ZIP uploaded

The zip file is present in the release assets and the URL can be copied from there.
The checksum can be found in the respective file in the checksums directory.
