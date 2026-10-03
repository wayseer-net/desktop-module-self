# Wayseer module: self

The `internal` module of Wayseer Desktop: Wayseer describing itself. It shows how fast the app
draws, how much memory it holds, its event bus, the state of every other module, and its log.
The app feeds it through a `Probe`, and it always has something to show, so a new config is
never empty.

```
go get wayseer.dev/modules/self
```

Wayseer links it in, so users do not install it. Its options and what it shows are in Wayseer's
user guide, under "Wayseer itself".

## Working on it

```
make check   # what CI runs: tests with the conformance suite, vet and lint for every platform, a key scan
make help    # every target
```

It imports only the SDK (`wayseer.dev/sdk`), the standard library and its own
dependencies; `TestImportsOnlyTheSDK` keeps it that way. golangci-lint is pinned in
`tools/go.mod`, and gitleaks runs at a pinned version through `go run`.

To change it alongside the SDK or the app, use a Go workspace: `go.work` here with
`use . ../../sdk`, or the app's `make workspace`, which writes one for the whole of Wayseer
Desktop. `go.work` is ignored by git.

Until `wayseer.dev` serves the SDK's page and its repository is public, fetching it needs
`GOPRIVATE=github.com/wayseer-net/*` and git access to GitHub over HTTPS.

## Licence

MIT; see `LICENSE`.
