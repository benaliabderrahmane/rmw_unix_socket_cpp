# Releasing

Binaries reach users through the ROS build farm. `bloom` generates debian
metadata into a separate `-release` repo and opens a PR against `ros/rosdistro`;
after it merges, the farm builds `ros-<distro>-rmw-unix-socket-cpp`. None of this
touches source on `main`.

```bash
sudo apt install python3-bloom python3-catkin-pkg
```

`bloom` also needs a GitHub token with `public_repo` and `workflow` scopes.

## Supported distros

`jazzy`, `kilted`, `rolling`, `lyrical` — one bloom track each, matching the CI
and release workflow matrices.

**Not Humble.** `src/serialization.cpp` uses
`eprosima::fastcdr::CdrVersion::DDS_CDR`, which is Fast CDR 2.x. Humble ships
Fast CDR 1.0.x, where that enum does not exist.

## Track answers (asked on each distro's `--new-track`)

| Prompt | Answer |
| --- | --- |
| Release repository url | `https://github.com/ros2-gbp/rmw_unix_socket_cpp-release.git` |
| Upstream repository uri | `https://github.com/benaliabderrahmane/rmw_unix_socket_cpp.git` |
| Upstream devel branch | the branch for that distro (`main` until per-distro branches exist) |
| Version | `:{auto}` |
| Release tag | `v:{version}` |

bloom does not create the release repository: request it through
`ros2-gbp/ros2-gbp-github-org` before the first release; empty is fine. Use that
one repository for every track. Rolling only accepts a release repository in the
`ros2-gbp` organisation, every ROS 2 distro accepts one there, and one repository
keeps all four tracks in one `tracks.yaml`.

The release tag matters: our tags are `v0.5.0` but `catkin_prepare_release`
writes `0.5.0`, so a bare `:{version}` makes bloom look for a tag that isn't
there.

## First release

Release 0.6.0 first; do not bloom the existing `v0.5.0`. `:{auto}` reads the
version from `main`'s `package.xml`, and `v0.5.0` predates `CHANGELOG.rst` and
the `<url>` tags - releasing it would ship exactly the empty changelog this setup
exists to avoid. Cut 0.6.0 as below, with `--bump minor`, then open each track:

```bash
bloom-release --new-track --rosdistro jazzy --track jazzy rmw_unix_socket_cpp
```

and the same for `kilted`, `rolling` and `lyrical`: each distro's first release
needs `--new-track`, and bloom asks the questions above for it. Only this first
rosdistro PR gets real scrutiny.

## Cutting a release

Work lands on `devel` and `main` is what gets released, so a release is a version
commit on `devel`, a release PR into `main`, then the tag.

Run it from a fresh clone. The `catkin_*` tools search the current directory for
`package.xml` and stop at a second copy of this package (a worktree, another
checkout below it), and `catkin_prepare_release` asks before releasing a tree
with untracked files.

```bash
git clone https://github.com/benaliabderrahmane/rmw_unix_socket_cpp.git
cd rmw_unix_socket_cpp && git switch devel   # every PR for the release merged, CI green
catkin_prepare_release --tag-prefix v --bump minor --no-push
                                             # Forthcoming -> 0.6.0 (date), package.xml 0.6.0, commit, local tag v0.6.0
git push origin devel                        # bypasses the devel ruleset: repo admins only
gh pr create --base main --head devel --title "Release v0.6.0"
                                             # merge it with a merge commit, then:
git push origin v0.6.0                       # release.yml builds and publishes it
bloom-release --rosdistro jazzy --track jazzy rmw_unix_socket_cpp   # once release.yml is green; per distro
```

`--bump`: `minor` when Forthcoming has an **Added** or **Changed** entry, `patch`
when it only fixes. The default is `patch`.

`--tag-prefix v` is not optional. `catkin_prepare_release` builds its tag as
`tag_prefix + version` and the prefix defaults to empty, so without it you get
`0.6.0` — which the `v:{version}` bloom track will not find, and which
`release.yml` ignores because it only triggers on `v*`.

`--no-push` holds the tag back until `main` has the release: bloom's upstream
branch is `main`, so before the merge its `:{auto}` version is still the old one.
If the release PR needs a fix first, land it on `devel`, then move the local tag
to the new tip (`git fetch origin && git tag -f v0.6.0 origin/devel`) before
pushing it.

Merge the release PR with a merge commit, as v0.5.0 was (#63). A squash gives
`main` a commit `devel` lacks, and the next release PR conflicts with it.

After the first release, no `--new-track` — bloom reuses the track's answers. The
resulting rosdistro PR changes one line, the `version:` field.

- One invocation and one rosdistro PR per distro.
- Versions are independent across distros but must increase monotonically within
  one.
- Rebuild with no source change: re-run `bloom-release` at the same version and
  the debian increment bumps, `0.6.0-1` → `0.6.0-2`.
- Never hand-edit or delete the `-release` repo. bloom regenerates it each time
  but needs its history.

Push the tag, let `release.yml` go green, then run bloom.

## CHANGELOG.rst

`CHANGELOG.rst` must sit next to `package.xml`; bloom reads nowhere else. Each PR
adds its entry under the `Forthcoming` heading at the top, creating that heading
(`Forthcoming` over `-----------`) above the newest version when a release has
just consumed it; `catkin_prepare_release` renames it to the new version and date.
Version headings are `0.5.0 (2026-08-27)` over a `------` underline, and a
version body may contain only paragraphs, bullet lists and transitions — REP-132
forbids sub-sections and `catkin_pkg` drops anything else silently, which ships
an empty or truncated debian changelog entry. Write `Added` / `Changed` /
`Fixed` as bold paragraphs, not headings. The `release-metadata` CI job checks
all of this, `Forthcoming` included.
