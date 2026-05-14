from importlib import import_module

import click


class LazyGroup(click.Group):
    def __init__(self, *args, lazy_subcommands=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.lazy_subcommands = lazy_subcommands or {}

    def list_commands(self, ctx):
        return sorted(set(super().list_commands(ctx)) | set(self.lazy_subcommands))

    def get_command(self, ctx, name):
        if name in self.lazy_subcommands:
            module_path, obj_name = self.lazy_subcommands[name].split(":")
            mod = import_module(module_path)
            return getattr(mod, obj_name)
        return super().get_command(ctx, name)


@click.group(
    cls=LazyGroup,
    lazy_subcommands={
        "project": "foundinspace.pipeline.project_cli:cli",
        "gaia": "foundinspace.pipeline.gaia.cli:cli",
        "gaia-to-hip": "foundinspace.pipeline.gaia_to_hip.cli:cli",
        "hip": "foundinspace.pipeline.hipparcos.cli:cli",
        "identifiers": "foundinspace.pipeline.identifiers.cli:cli",
        "merge": "foundinspace.pipeline.merge.cli:cli",
        "overrides": "foundinspace.pipeline.overrides.cli:cli",
    },
)
def cli():
    pass
