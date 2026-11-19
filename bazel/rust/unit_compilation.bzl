"""Precompile unit binaries without runtime-data builds competing for resources."""

def _unit_compilation_impl(target, ctx):
    if ctx.rule.kind != "rust_test" or ctx.rule.attr.crate == None:
        return []

    return [OutputGroupInfo(unit_compilation = target[DefaultInfo].files)]

unit_compilation = aspect(implementation = _unit_compilation_impl)
