# Change Log

## [v3.0.0](https://github.com/NASA-PDS-Incubator/registry/tree/v3.0.0) (2020-03-26)
[Full Changelog](https://github.com/NASA-PDS-Incubator/registry/compare/v2.2.1...v3.0.0)

**Implemented enhancements:**

- Update Registry documentation per new registry-mgr upgrades [\#38](https://github.com/NASA-PDS-Incubator/registry/issues/38)
- Expand Registry collection to include additional overall data system metrics on the archive [\#31](https://github.com/NASA-PDS-Incubator/registry/issues/31)

**Merged pull requests:**

- Refactor registry to include enhanced registry manager and standalone registry [\#39](https://github.com/NASA-PDS-Incubator/registry/pull/39) ([jordanpadams](https://github.com/jordanpadams))
- Rolled back some changes from issue \#i31 [\#35](https://github.com/NASA-PDS-Incubator/registry/pull/35) ([tdddblog](https://github.com/tdddblog))
- More system metrics \(file info\). See Issue \#31 [\#34](https://github.com/NASA-PDS-Incubator/registry/pull/34) ([tdddblog](https://github.com/tdddblog))

## [v2.2.1](https://github.com/NASA-PDS-Incubator/registry/tree/v2.2.1) (2019-10-27)
[Full Changelog](https://github.com/NASA-PDS-Incubator/registry/compare/v2.2.0...v2.2.1)

## [v2.2.0](https://github.com/NASA-PDS-Incubator/registry/tree/v2.2.0) (2019-10-25)
[Full Changelog](https://github.com/NASA-PDS-Incubator/registry/compare/v2.1.2...v2.2.0)

**Implemented enhancements:**

- Add schema for XPath to prevent failure on ingestions with differing data types [\#28](https://github.com/NASA-PDS-Incubator/registry/issues/28)
- Update search-post tool to accomplish all functionality that Solr Post Tool does [\#27](https://github.com/NASA-PDS-Incubator/registry/issues/27)
- Upgrade to Solr 8.2.0 [\#25](https://github.com/NASA-PDS-Incubator/registry/issues/25)
- Add new search\_post utility for posting Solr Docs to the search index [\#23](https://github.com/NASA-PDS-Incubator/registry/issues/23)
- Update Registry documentation per collection updates and report-mgr tool [\#20](https://github.com/NASA-PDS-Incubator/registry/issues/20)

**Fixed bugs:**

- Fix docker and standalone installers to work with new collections [\#22](https://github.com/NASA-PDS-Incubator/registry/issues/22)

**Merged pull requests:**

- Update docker installer and search-post to allow for help command [\#30](https://github.com/NASA-PDS-Incubator/registry/pull/30) ([jordanpadams](https://github.com/jordanpadams))
- Upgrade to Solr 8.2.0 and create explicit XPath collection config [\#29](https://github.com/NASA-PDS-Incubator/registry/pull/29) ([jordanpadams](https://github.com/jordanpadams))
- Add new search\_post utility for posting Solr Docs to the search index [\#21](https://github.com/NASA-PDS-Incubator/registry/pull/21) ([tdddblog](https://github.com/tdddblog))

## [v2.1.2](https://github.com/NASA-PDS-Incubator/registry/tree/v2.1.2) (2019-10-19)
[Full Changelog](https://github.com/NASA-PDS-Incubator/registry/compare/v2.1.1...v2.1.2)

## [v2.1.1](https://github.com/NASA-PDS-Incubator/registry/tree/v2.1.1) (2019-10-17)
[Full Changelog](https://github.com/NASA-PDS-Incubator/registry/compare/v2.1.0...v2.1.1)

**Fixed bugs:**

- Fix bug in registry installer occurring from open sourcing or project [\#18](https://github.com/NASA-PDS-Incubator/registry/issues/18)

**Merged pull requests:**

- Fix bug in registry installer occurring from open sourcing or project [\#19](https://github.com/NASA-PDS-Incubator/registry/pull/19) ([jordanpadams](https://github.com/jordanpadams))

## [v2.1.0](https://github.com/NASA-PDS-Incubator/registry/tree/v2.1.0) (2019-10-15)
**Implemented enhancements:**

- Add capability to handle tracking of Harvest ingestions [\#17](https://github.com/NASA-PDS-Incubator/registry/issues/17)
- Create registry blob collection [\#10](https://github.com/NASA-PDS-Incubator/registry/issues/10)
- Review and cleanup applicable search fields for display and index output [\#4](https://github.com/NASA-PDS-Incubator/registry/issues/4)
- Enhance usability of search endpoints by renaming and simplifying their definitions [\#3](https://github.com/NASA-PDS-Incubator/registry/issues/3)
- Enhance registry schema and config for refined search capabilities [\#2](https://github.com/NASA-PDS-Incubator/registry/issues/2)

**Closed issues:**

- Update Maven docs to refer to Github release assets [\#1](https://github.com/NASA-PDS-Incubator/registry/issues/1)

**Merged pull requests:**

- Added transaction\_id to registry and PDS collections [\#16](https://github.com/NASA-PDS-Incubator/registry/pull/16) ([tdddblog](https://github.com/tdddblog))
- Issue \#2: Schema cleanup and fixes [\#13](https://github.com/NASA-PDS-Incubator/registry/pull/13) ([tdddblog](https://github.com/tdddblog))
- Registry \#10: Create new registry blob collection to replace .system [\#12](https://github.com/NASA-PDS-Incubator/registry/pull/12) ([tdddblog](https://github.com/tdddblog))
- Issue \#3 Renamed and simplified request handler definition. Cleanup. [\#11](https://github.com/NASA-PDS-Incubator/registry/pull/11) ([tdddblog](https://github.com/tdddblog))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*