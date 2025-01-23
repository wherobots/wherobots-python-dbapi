# Contributor Guidance

## Development

Note: this project is explicitly meant to support Python versions as old
as 3.8. This (unfortunately) means it requires Poetry 1.8.x to install
and build correctly.

```
$ brew install pipx
$ pipx ensurepath
$ pipx install poetry==1.8.5
$ poetry update
```

## Publish package to PyPI

When we are ready to release a new version `vx.y.z`, one of the maintainers should:

1. Execute `poetry version {minor,patch}` to update the project version
1. Commit the version change and any release-related changes
1. Create a new tag with `git tag -s -m "Version x.y.z" vx.y.z`.
   If the tag doesn't match with the project version, step 4 validation will fail.
1. Push the tag to the repo with `git push origin vx.y.z`
1. There will be a GitHub Action triggered automatically which will build and publish to PyPI
