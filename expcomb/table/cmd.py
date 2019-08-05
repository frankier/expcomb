import click
from io import StringIO
from subprocess import call
from pylatex import Document, NoEscape, Package

from expcomb.filter import empty_filter
from .utils import docs_from_dbs


def add_all_tables(group, tables, pk_extra):

    @group.command()
    @click.pass_context
    @click.argument("db_paths", type=click.Path(), nargs=-1)
    @click.option("--preview/--no-preview")
    def all_tables(ctx, db_paths, preview):
        if preview:
            latex_doc = Document(
                geometry_options={"paperwidth": "100cm", "paperheight": "100cm"}
            )
            latex_doc.packages.append(Package("tabu"))
            latex_doc.packages.append(Package("booktabs"))
            latex_doc.packages.append(Package("multirow"))

        for table_tpl in tables:
            name, spec = table_tpl[:2]
            if len(table_tpl) > 2:
                filter = table_tpl[2]
            else:
                filter = empty_filter
            docs = docs_from_dbs(db_paths, filter, pk_extra)

            table = StringIO()
            spec.print(docs, outf=table)
            if preview:
                latex_doc.append(NoEscape(table.getvalue()))
                latex_doc.append(NoEscape("\\clearpage"))
            print(table.getvalue())

        if preview:
            latex_doc.generate_pdf()
            call(["evince", "default_filepath.pdf"])
