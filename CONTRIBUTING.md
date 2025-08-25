# Contributing

## Development

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run tests: `poetry run pytest`
5. Run pre-commit hooks: `poetry run pre-commit run -a`
6. Commit your changes using [Conventional Commits](https://www.conventionalcommits.org/)
7. Push to your branch: `git push origin feature/my-feature`
8. Create a Pull Request

## Releasing

Releases are done automatically using GitHub Actions on every commit to `master`.
The workflow will create a `release/x.y.z` branch and an associated pull request.
After review of the release and merging the pull request, the release will be published as a tag
and a GitHub release will be created.
