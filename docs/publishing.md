# Publishing this repository

The standalone directory is a separate Git repository with its own initial history. The original lab checkout is not a remote or dependency.

Once GitHub CLI is installed and signed in, create a repository under the desired account or organization:

```sh
gh auth login
gh repo create OWNER/ringstore --public --source . --remote origin --push \
  --description "Replicated Rust storage with consistent hashing, operation logs, and restartable anti-entropy repair"
```

Replace `OWNER` with your account or organization. Use `--private` instead of `--public` for private visibility. Choose an unused repository name if `ringstore` already exists.

Alternatively, create an empty repository through GitHub's website, without generating a README, license, or `.gitignore`, then connect its SSH remote:

```sh
git remote add origin git@github.com:OWNER/ringstore.git
git push -u origin main
```

The workflow under `.github/workflows/ci.yml` runs after the push. Suitable repository topics are `rust`, `distributed-systems`, `consistent-hashing`, `replication`, `anti-entropy`, and `fault-tolerance`. The README is the landing page; architecture, design decisions, and test evidence link from it. See `ACKNOWLEDGEMENTS.md` for retained/adapted material and provenance.
