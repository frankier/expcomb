import click
from expcomb.sigtest.bootstrap import bootstrap
from expcomb.sigtest.disp import disp


merged = click.CommandCollection(
    sources=[bootstrap, disp], help="Commands for significance testing of guess"
)
