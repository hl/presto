---
name: Release Checklist
about: Checklist for preparing and publishing a new release
title: 'Release v[VERSION]'
labels: 'release'
assignees: ''
---

## Release Checklist for v[VERSION]

### Pre-Release
- [ ] All planned features/fixes are merged
- [ ] All tests are passing
- [ ] Documentation is up to date
- [ ] Performance benchmarks are acceptable
- [ ] Breaking changes are documented

### Version Preparation
- [ ] Run "Prepare Release" workflow with version and changelog
- [ ] Review the generated PR
- [ ] Merge the release preparation PR
- [ ] Create and push git tag: `git tag v[VERSION] && git push origin v[VERSION]`

### Publishing
- [ ] Automated publishing workflow completes successfully
- [ ] Package appears on [Hex.pm](https://hex.pm/packages/ex_presto)
- [ ] Documentation is published to [HexDocs](https://hexdocs.pm/ex_presto)
- [ ] GitHub release is created

### Post-Release
- [ ] Announce release (if significant)
- [ ] Update any dependent projects
- [ ] Monitor for issues

### Notes
<!-- Add any special notes for this release -->

---

**Release Commands:**
```bash
# After merging release PR
git checkout main
git pull origin main
git tag v[VERSION]
git push origin v[VERSION]
```