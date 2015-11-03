The cmake rules in this directory override the ones in cmake/
i.e. if ExternalProjectBoost.cmake exists in both cmake/ and override_cmake/
only the ones in override_cmake/ will be included.

This is a staging place for changes to the dato-deps dependency builder
which does not require updating the entire toolchain.
