# Releasing

Binaries reach users through the ROS build farm. `bloom` generates debian
metadata into a separate `-release` repo and opens a PR against `ros/rosdistro`;
after it merges, the farm builds `ros-<distro>-rmw-unix-socket-cpp`. None of this
touches source on `main`.

```bash
sudo apt install python3-bloom python3-catkin-pkg
```

`bloom` also needs a GitHub token with `public_repo` scope.

## Supported distros

`jazzy`, `kilted`, `rolling`, `lyrical` — one bloom track each, matching the CI
and release workflow matrices.

**Not Humble.** `src/serialization.cpp` uses
`eprosima::fastcdr::CdrVersion::DDS_CDR`, which is Fast CDR 2.x. Humble ships
Fast CDR 1.0.x, where that enum does not exist.

## Track answers (asked once, on `--new-track`)

| Prompt | Answer |
| --- | --- |
| Release repository url | yes — let bloom create `…/rmw_unix_socket_cpp-release` |
| Upstream repository uri | `https://github.com/benaliabderrahmane/rmw_unix_socket_cpp.git` |
| Upstream devel branch | the branch for that distro (`main` until per-distro branches exist) |
| Version | `:{auto}` |
| Release tag | `v:{version}` |

The release tag matters: our tags are `v0.5.0` but `catkin_prepare_release`
writes `0.5.0`, so a bare `:{version}` makes bloom look for a tag that isn't
there.

## First release

```bash
bloom-release --new-track --rosdistro jazzy --track jazzy rmw_unix_socket_cpp
```

Only this first rosdistro PR gets real scrutiny.

## Every release after that

```bash
catkin_generate_changelog     # appends unreleased commits to rmw_unix_socket_cpp/CHANGELOG.rst
$EDITOR rmw_unix_socket_cpp/CHANGELOG.rst
git commit -am "Update changelog"
catkin_prepare_release        # bumps package.xml, commits, tags, pushes
bloom-release --rosdistro jazzy --track jazzy rmw_unix_socket_cpp
```

No `--new-track` — bloom reuses the answers above. The resulting rosdistro PR
changes one line, the `version:` field.

- One invocation and one rosdistro PR per distro.
- Versions are independent across distros but must increase monotonically within
  one.
- Rebuild with no source change: re-run `bloom-release` at the same version and
  the debian increment bumps, `0.6.0-1` → `0.6.0-2`.
- Never hand-edit or delete the `-release` repo. bloom regenerates it each time
  but needs its history.

Tag first, let `release.yml` go green, then run bloom.

## CHANGELOG.rst

`CHANGELOG.rst` must sit next to `package.xml`; bloom reads nowhere else.
Version headings are `0.5.0 (2026-08-27)` over a `------` underline, and a
version body may contain only paragraphs and bullet lists — REP-132 forbids
sub-sections and `catkin_pkg` drops them silently, which ships an empty debian
changelog entry. The `release-metadata` CI job checks both.
