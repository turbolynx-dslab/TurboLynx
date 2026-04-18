# turbolynx (Python)

TurboLynx Python bindings packaged as a pre-built wheel.

## Build

```bash
cd tools/pythonpkg
TURBOLYNX_BUILD_DIR=../../build pip wheel . -w dist/
```

## License

The wheel metadata is published as `Apache-2.0`, and the wheel also includes
the preserved third-party notices and bundled license texts required by the
TurboLynx runtime. See `LICENSE`, `THIRD-PARTY-NOTICES.md`, and `licenses/`
inside the built artifact.
